/* eslint-disable max-len */
const nock = require('nock');
const config = require('config');
const chai = require('chai');
const amqp = require('amqplib');
const uuid = require('uuid');
const sleep = require('sleep');
const { task } = require('rw-doc-importer-messages');
const RabbitMQConnectionError = require('errors/rabbitmq-connection.error');
const { createMockGetDataset } = require('./utils/helpers');
const { getTestServer } = require('./utils/test-server');
const { USERS } = require('./utils/test.constants');

chai.should();

const requester = getTestServer();
let rabbitmqConnection = null;
let channel;

nock.disableNetConnect();
nock.enableNetConnect((host) => [`${process.env.HOST_IP}:${process.env.PORT}`, process.env.ELASTIC_TEST_URL].includes(host));

describe('Query datasets - Delete queries', () => {

    before(async () => {
        if (process.env.NODE_ENV !== 'test') {
            throw Error(`Running the test suite with NODE_ENV ${process.env.NODE_ENV} may result in permanent data loss. Please use NODE_ENV=test.`);
        }

        let connectAttempts = 10;
        while (connectAttempts >= 0 && rabbitmqConnection === null) {
            try {
                rabbitmqConnection = await amqp.connect(config.get('rabbitmq.url'));
            } catch (err) {
                connectAttempts -= 1;
                await sleep.sleep(5);
            }
        }
        if (!rabbitmqConnection) {
            throw new RabbitMQConnectionError();
        }
    });

    beforeEach(async () => {
        channel = await rabbitmqConnection.createConfirmChannel();

        await channel.assertQueue(config.get('queues.tasks'));
        await channel.purgeQueue(config.get('queues.tasks'));

        const tasksQueueStatus = await channel.checkQueue(config.get('queues.tasks'));
        tasksQueueStatus.messageCount.should.equal(0);
    });

    it('Doing a delete query without being authenticated should return a 403 error', async () => {
        const datasetId = uuid.v4();

        createMockGetDataset(datasetId);

        const query = `delete from ${datasetId} where 1=1`;

        nock(process.env.GATEWAY_URL)
            .get(`/v1/convert/sql2SQL`)
            .query({
                sql: query
            })
            .once()
            .reply(200, {
                data: {
                    type: 'result',
                    attributes: {
                        query: 'DELETE FROM 123 WHERE 1 = 1',
                        jsonSql: {
                            from: '123',
                            delete: true,
                            where: [
                                {
                                    type: 'operator',
                                    value: '=',
                                    left: [
                                        {
                                            value: 1,
                                            type: 'number'
                                        }
                                    ],
                                    right: [
                                        {
                                            value: 1,
                                            type: 'number'
                                        }
                                    ]
                                }
                            ]
                        }
                    }
                }
            });

        const queryResponse = await requester
            .post(`/api/v1/document/query/csv/${datasetId}?sql=${encodeURI(query)}`)
            .send();

        queryResponse.status.should.equal(403);
        queryResponse.body.should.have.property('errors').and.be.an('array');
        queryResponse.body.errors[0].should.have.property('status').and.equal(403);
        queryResponse.body.errors[0].should.have.property('detail').and.equal('Not authorized to execute DELETE query');
    });

    it('Doing a delete query while being authenticated should return 204 (happy case)', async () => {
        const requestBody = {
            loggedUser: USERS.ADMIN
        };

        const datasetId = uuid.v4();

        createMockGetDataset(datasetId);

        const query = `delete from ${datasetId} where 1=1`;

        nock(process.env.GATEWAY_URL)
            .get(`/v1/convert/sql2SQL`)
            .query({
                sql: query
            })
            .once()
            .reply(200, {
                data: {
                    type: 'result',
                    attributes: {
                        query: 'DELETE FROM 123 WHERE 1 = 1',
                        jsonSql: {
                            from: '123',
                            delete: true,
                            where: [
                                {
                                    type: 'operator',
                                    value: '=',
                                    left: [
                                        {
                                            value: 1,
                                            type: 'number'
                                        }
                                    ],
                                    right: [
                                        {
                                            value: 1,
                                            type: 'number'
                                        }
                                    ]
                                }
                            ]
                        }
                    }
                }
            });

        const queryResponse = await requester
            .post(`/api/v1/document/query/csv/${datasetId}`)
            .query({
                sql: query
            })
            .send(requestBody);

        queryResponse.status.should.equal(204);

        let expectedStatusQueueMessageCount = 1;

        const validateStatusQueueMessages = (resolve) => async (msg) => {
            const content = JSON.parse(msg.content.toString());

            content.should.have.property('type').and.equal(task.MESSAGE_TYPES.TASK_DELETE);
            content.should.have.property('datasetId').and.equal(`${datasetId}`);
            content.should.have.property('id');
            content.should.have.property('index').and.equal('test_index_d1ced4227cd5480a8904d3410d75bf42_1587619728489');
            content.should.have.property('query').and.equal(`DELETE FROM test_index_d1ced4227cd5480a8904d3410d75bf42_1587619728489 WHERE 1 = 1`);

            await channel.ack(msg);

            expectedStatusQueueMessageCount -= 1;

            if (expectedStatusQueueMessageCount === 0) {
                resolve();
            }
        };

        return new Promise((resolve) => {
            channel.consume(config.get('queues.tasks'), validateStatusQueueMessages(resolve));
        });
    });

    after(() => {
        if (!nock.isDone()) {
            const pendingMocks = nock.pendingMocks();
            throw new Error(`Not all nock interceptors were used: ${pendingMocks}`);
        }

        rabbitmqConnection.close();
    });
});

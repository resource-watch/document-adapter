const nock = require('nock');
const deepEqualInAnyOrder = require('deep-equal-in-any-order');
const RabbitMQConnectionError = require('errors/rabbitmq-connection.error');
const chai = require('chai');
const amqp = require('amqplib');
const config = require('config');
const sleep = require('sleep');
const { getTestServer } = require('./utils/test-server');
const { mockGetUserFromToken } = require('./utils/helpers');
const { USERS } = require('./utils/test.constants');

chai.should();
chai.use(deepEqualInAnyOrder);

let requester;
let rabbitmqConnection = null;
let channel;

nock.disableNetConnect();
nock.enableNetConnect((host) => [`${process.env.HOST_IP}:${process.env.PORT}`, process.env.ELASTIC_TEST_URL].includes(host));

describe('Dataset delete tests', () => {

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

        requester = await getTestServer();
    });

    beforeEach(async () => {
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

        channel = await rabbitmqConnection.createConfirmChannel();
        await channel.assertQueue(config.get('queues.tasks'));
        await channel.purgeQueue(config.get('queues.tasks'));

        const tasksQueueStatus = await channel.checkQueue(config.get('queues.tasks'));
        tasksQueueStatus.messageCount.should.equal(0);
    });

    it('Delete dataset index with tableName null should nothing to do and return success (happy case)', async () => {
        mockGetUserFromToken(USERS.USER);

        const datasetId = new Date().getTime();

        nock(process.env.GATEWAY_URL).get(`/v1/dataset/${datasetId}`).reply(200, {
            data: {
                attributes: {
                    tableName: null
                }
            }
        });

        const response = await requester
            .delete(`/api/v1/document/json/${datasetId}`)
            .set('Authorization', `Bearer abcd`);

        response.status.should.equal(200);
    });

    it('Delete dataset index should be successful (happy case)', async () => {
        mockGetUserFromToken(USERS.USER);
        const datasetId = new Date().getTime();

        nock(process.env.GATEWAY_URL).get(`/v1/dataset/${datasetId}`).reply(200, {
            data: {
                attributes: {
                    tableName: 'test'
                }
            }
        });

        const response = await requester
            .delete(`/api/v1/document/json/${datasetId}`)
            .set('Authorization', `Bearer abcd`);

        response.status.should.equal(200);

        let expectedStatusQueueMessageCount = 1;

        const validateStatusQueueMessages = (resolve) => async (msg) => {
            const content = JSON.parse(msg.content.toString());

            content.should.have.property('index').and.equal('test');
            content.should.have.property('datasetId').and.equal(`${datasetId}`);

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

    afterEach(async () => {
        await channel.assertQueue(config.get('queues.tasks'));
        const tasksQueueStatus = await channel.checkQueue(config.get('queues.tasks'));
        tasksQueueStatus.messageCount.should.equal(0);

        if (!nock.isDone()) {
            const pendingMocks = nock.pendingMocks();
            if (pendingMocks.length > 1) {
                throw new Error(`Not all nock interceptors were used: ${pendingMocks}`);
            }
        }

        await channel.close();
        channel = null;

        await rabbitmqConnection.close();
        rabbitmqConnection = null;
    });
});

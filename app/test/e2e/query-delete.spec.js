/* eslint-disable no-unused-vars,no-undef,max-len */
const nock = require('nock');
const config = require('config');
const chai = require('chai');
const amqp = require('amqplib');
const sleep = require('sleep');
const { task } = require('rw-doc-importer-messages');
const { getTestServer } = require('./test-server');
const { ROLES } = require('./test.constants');

const should = chai.should();

const requester = getTestServer();
let rabbitmqConnection = null;
let channel;

const elasticUri = process.env.ELASTIC_URI || `${config.get('elasticsearch.host')}:${config.get('elasticsearch.port')}`;

nock.disableNetConnect();
nock.enableNetConnect(`${process.env.HOST_IP}:${process.env.PORT}`);

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

        // process.on('unhandledRejection', should.fail);
        process.on('unhandledRejection', (error) => {
            console.log(error);
            should.fail(error);
        });
    });

    beforeEach(async () => {
        channel = await rabbitmqConnection.createConfirmChannel();

        await channel.assertQueue(config.get('queues.tasks'));
        await channel.purgeQueue(config.get('queues.tasks'));

        const tasksQueueStatus = await channel.checkQueue(config.get('queues.tasks'));
        tasksQueueStatus.messageCount.should.equal(0);
    });

    it('Doing a delete query without being authenticated should return a 403 error', async () => {
        const requestBody = {
            dataset: {
                name: 'Food Demand',
                slug: 'Food-Demand_3',
                type: null,
                subtitle: null,
                application: [
                    'rw'
                ],
                dataPath: 'data',
                attributesPath: null,
                connectorType: 'document',
                provider: 'json',
                userId: '1a10d7c6e0a37126611fd7a7',
                connectorUrl: 'http://gfw2-data.s3.amazonaws.com/alerts-tsv/output/to-api/output.json',
                tableName: 'index_051364f0fe4446c2bf95fa4b93e2dbd2_1536899613926',
                status: 'saved',
                published: true,
                overwrite: false,
                verified: false,
                blockchain: {},
                mainDateField: null,
                env: 'production',
                geoInfo: false,
                protected: false,
                legend: {
                    date: [],
                    region: [],
                    country: [],
                    nested: []
                },
                clonedHost: {},
                errorMessage: '',
                taskId: '/v1/doc-importer/task/986bd4ee-0bfe-4002-ae17-1d1594dffd0a',
                updatedAt: '2018-09-14T04:33:48.838Z',
                dataLastUpdated: null,
                widgetRelevantProps: [],
                layerRelevantProps: [],
                id: '051364f0-fe44-46c2-bf95-fa4b93e2dbd2'
            },
            loggedUser: null
        };

        const query = `delete from ${requestBody.dataset.id} where 1=1`;

        const results = [
            {
                _index: 'index_051364f0fe4446c2bf95fa4b93e2dbd2_1536899613926',
                _type: 'type',
                _id: 'kDZb1mUBbvDJlUQCLHRu',
                _score: 1,
                _source: {
                    thresh: 75, iso: 'USA', adm1: 27, adm2: 1641, area: 41420.47960515353
                }
            }, {
                _index: 'index_051364f0fe4446c2bf95fa4b93e2dbd2_1536899613926',
                _type: 'type',
                _id: 'mzZb1mUBbvDJlUQCLHRu',
                _score: 1,
                _source: {
                    thresh: 75, iso: 'BRA', adm1: 12, adm2: 1450, area: 315602.3928570104
                }
            }, {
                _index: 'index_051364f0fe4446c2bf95fa4b93e2dbd2_1536899613926',
                _type: 'type',
                _id: 'nDZb1mUBbvDJlUQCLHRu',
                _score: 1,
                _source: {
                    thresh: 75, iso: 'RUS', adm1: 35, adm2: 925, area: 1137359.9711284428
                }
            }, {
                _index: 'index_051364f0fe4446c2bf95fa4b93e2dbd2_1536899613926',
                _type: 'type',
                _id: 'oTZb1mUBbvDJlUQCLHRu',
                _score: 1,
                _source: {
                    thresh: 75, iso: 'COL', adm1: 30, adm2: 1017, area: 128570.48945388374
                }
            }
        ];

        nock(process.env.CT_URL)
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
            .post(`/api/v1/document/query/${requestBody.dataset.id}?sql=${encodeURI(query)}`)
            .send(requestBody);

        queryResponse.status.should.equal(403);
        queryResponse.body.should.have.property('errors').and.be.an('array');
        queryResponse.body.errors[0].should.have.property('status').and.equal(403);
        queryResponse.body.errors[0].should.have.property('detail').and.equal('Not authorized to execute DELETE query');
    });

    it('Doing a delete query while being authenticated should return 204 (happy case)', async () => {
        const requestBody = {
            dataset: {
                name: 'Food Demand',
                slug: 'Food-Demand_3',
                type: null,
                subtitle: null,
                application: [
                    'rw'
                ],
                dataPath: 'data',
                attributesPath: null,
                connectorType: 'document',
                provider: 'json',
                userId: '1a10d7c6e0a37126611fd7a7',
                connectorUrl: 'http://gfw2-data.s3.amazonaws.com/alerts-tsv/output/to-api/output.json',
                tableName: 'index_051364f0fe4446c2bf95fa4b93e2dbd2_1536899613926',
                status: 'saved',
                published: true,
                overwrite: false,
                verified: false,
                blockchain: {},
                mainDateField: null,
                env: 'production',
                geoInfo: false,
                protected: false,
                legend: {
                    date: [],
                    region: [],
                    country: [],
                    nested: []
                },
                clonedHost: {},
                errorMessage: '',
                taskId: '/v1/doc-importer/task/986bd4ee-0bfe-4002-ae17-1d1594dffd0a',
                updatedAt: '2018-09-14T04:33:48.838Z',
                dataLastUpdated: null,
                widgetRelevantProps: [],
                layerRelevantProps: [],
                id: '051364f0-fe44-46c2-bf95-fa4b93e2dbd2'
            },
            loggedUser: ROLES.ADMIN
        };

        const query = `delete from ${requestBody.dataset.id} where 1=1`;

        const results = [
            {
                _index: 'index_051364f0fe4446c2bf95fa4b93e2dbd2_1536899613926',
                _type: 'type',
                _id: 'kDZb1mUBbvDJlUQCLHRu',
                _score: 1,
                _source: {
                    thresh: 75, iso: 'USA', adm1: 27, adm2: 1641, area: 41420.47960515353
                }
            }, {
                _index: 'index_051364f0fe4446c2bf95fa4b93e2dbd2_1536899613926',
                _type: 'type',
                _id: 'mzZb1mUBbvDJlUQCLHRu',
                _score: 1,
                _source: {
                    thresh: 75, iso: 'BRA', adm1: 12, adm2: 1450, area: 315602.3928570104
                }
            }, {
                _index: 'index_051364f0fe4446c2bf95fa4b93e2dbd2_1536899613926',
                _type: 'type',
                _id: 'nDZb1mUBbvDJlUQCLHRu',
                _score: 1,
                _source: {
                    thresh: 75, iso: 'RUS', adm1: 35, adm2: 925, area: 1137359.9711284428
                }
            }, {
                _index: 'index_051364f0fe4446c2bf95fa4b93e2dbd2_1536899613926',
                _type: 'type',
                _id: 'oTZb1mUBbvDJlUQCLHRu',
                _score: 1,
                _source: {
                    thresh: 75, iso: 'COL', adm1: 30, adm2: 1017, area: 128570.48945388374
                }
            }
        ];

        nock(process.env.CT_URL)
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
            .post(`/api/v1/document/query/${requestBody.dataset.id}`)
            .query({
                sql: query
            })
            .send(requestBody);

        queryResponse.status.should.equal(204);

        await new Promise(resolve => setTimeout(resolve, 3000));

        const postQueueStatus = await channel.assertQueue(config.get('queues.tasks'));
        postQueueStatus.messageCount.should.equal(1);

        const validateMessage = async (msg) => {
            const content = JSON.parse(msg.content.toString());
            content.should.have.property('type').and.equal(task.MESSAGE_TYPES.TASK_DELETE);
            content.should.have.property('datasetId').and.equal(requestBody.dataset.id);
            content.should.have.property('id');
            content.should.have.property('index').and.equal(requestBody.dataset.tableName);
            content.should.have.property('query').and.equal(`DELETE FROM index_051364f0fe4446c2bf95fa4b93e2dbd2_1536899613926 WHERE 1 = 1`);

            await channel.ack(msg);
        };

        await channel.consume(config.get('queues.tasks'), validateMessage.bind(this));
    });

    after(() => {
        if (!nock.isDone()) {
            const pendingMocks = nock.pendingMocks();
            if (pendingMocks.length > 1) {
                throw new Error(`Not all nock interceptors were used: ${pendingMocks}`);
            }
        }

        rabbitmqConnection.close();
        process.removeListener('unhandledRejection', should.fail);
    });
});

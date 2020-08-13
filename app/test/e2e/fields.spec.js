/* eslint-disable no-unused-vars,no-undef,no-await-in-loop */
const nock = require('nock');
const deepEqualInAnyOrder = require('deep-equal-in-any-order');
const RabbitMQConnectionError = require('errors/rabbitmq-connection.error');
const chai = require('chai');
const amqp = require('amqplib');
const config = require('config');
const sleep = require('sleep');
const { task } = require('rw-doc-importer-messages');
const { getTestServer } = require('./utils/test-server');
const { ROLES } = require('./utils/test.constants');

const should = chai.should();
chai.use(deepEqualInAnyOrder);

const requester = getTestServer();
let rabbitmqConnection = null;
let channel;

nock.disableNetConnect();
nock.enableNetConnect(`${process.env.HOST_IP}:${process.env.PORT}`);

const elasticUri = process.env.ELASTIC_URI || `${config.get('elasticsearch.host')}:${config.get('elasticsearch.port')}`;

describe('GET dataset fields', () => {

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

        process.on('unhandledRejection', should.fail);
        // process.on('unhandledRejection', (error) => {
        //     console.log(error);
        //     should.fail(error);
        // });
    });

    beforeEach(async () => {
        channel = await rabbitmqConnection.createConfirmChannel();

        await channel.assertQueue(config.get('queues.tasks'));
        await channel.purgeQueue(config.get('queues.tasks'));

        const tasksQueueStatus = await channel.checkQueue(config.get('queues.tasks'));
        tasksQueueStatus.messageCount.should.equal(0);
    });

    it('Append a dataset without user should return an error', async () => {
        const fieldsStructure = {
            adm1: { type: 'keyword' },
            avg_biomass_per_ha: { type: 'double' },
            aze: { type: 'keyword' },
            extent_2000: { type: 'double' },
            extent_2010: { type: 'double' },
            global_land_cover: { type: 'keyword' },
            idn_forest_moratorium: { type: 'keyword' },
            ifl: { type: 'keyword' },
            iso: { type: 'keyword' },
            kba: { type: 'keyword' },
            land_right: { type: 'keyword' },
            landmark: { type: 'keyword' },
            managed_forests: { type: 'keyword' },
            mining: { type: 'keyword' },
            oil_palm: { type: 'keyword' },
            plantations: { type: 'keyword' },
            primary_forest: { type: 'keyword' },
            resource_right: { type: 'keyword' },
            tcs: { type: 'keyword' },
            threshold: { type: 'integer' },
            tiger_cl: { type: 'keyword' },
            total_area: { type: 'double' },
            total_biomass: { type: 'double' },
            total_co2: { type: 'double' },
            total_gain: { type: 'double' },
            water_stress: { type: 'keyword' },
            wdpa: { type: 'keyword' },
            weighted_biomass_per_ha: { type: 'float' },
            wood_fiber: { type: 'keyword' },
            year_data: {
                type: 'nested',
                include_in_parent: true,
                properties: {
                    area_loss: { type: 'float' },
                    biomass_loss: { type: 'float' },
                    carbon_emissions: { type: 'float' },
                    year: { type: 'long' }
                }
            }
        };

        nock(`http://${elasticUri}`)
            .get('/index_ef7d64c631664053a0b7e221d84496a5_1575556546107/_mapping')
            .reply(200, {
                index_ef7d64c631664053a0b7e221d84496a5_1575556546107: {
                    mappings: {
                        type: {
                            properties: fieldsStructure
                        }
                    }
                }
            });

        const timestamp = new Date().getTime();

        const response = await requester
            .post(`/api/v1/document/fields/${timestamp}`)
            .send({
                dataset: {
                    data: {
                        id: timestamp,
                        type: 'dataset',
                        attributes: {
                            name: 'Tree Cover Loss 2018 Summary - GADM Adm1 level - v20190701',
                            slug: 'Tree-Cover-Loss-2018-Summary-GADM-Adm1-level-v20190701',
                            type: null,
                            subtitle: null,
                            application: [
                                'gfw'
                            ],
                            dataPath: null,
                            attributesPath: null,
                            connectorType: 'document',
                            provider: 'json',
                            userId: 'microservice',
                            connectorUrl: null,
                            sources: [
                                'https://gfw-files.s3.amazonaws.com/2018_update/results/annualupdate_minimal_20190701_1825/adm1/adm1-part-0000.json',
                                'https://gfw-files.s3.amazonaws.com/2018_update/results/annualupdate_minimal_20190701_1825/adm1/adm1-part-0002.json',
                                'https://gfw-files.s3.amazonaws.com/2018_update/results/annualupdate_minimal_20190701_1825/adm1/adm1-part-0003.json',
                                'https://gfw-files.s3.amazonaws.com/2018_update/results/annualupdate_minimal_20190701_1825/adm1/adm1-part-0001.json'
                            ],
                            tableName: 'index_ef7d64c631664053a0b7e221d84496a5_1575556546107',
                            status: 'saved',
                            published: true,
                            overwrite: true,
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
                                nested: [
                                    'year_data'
                                ],
                                integer: [
                                    'threshold'
                                ],
                                short: [],
                                byte: [],
                                double: [
                                    'total_area',
                                    'extent_2000',
                                    'extent_2010',
                                    'total_gain',
                                    'total_biomass',
                                    'avg_biomass_per_ha',
                                    'total_co2'
                                ],
                                float: [],
                                half_float: [],
                                scaled_float: [],
                                boolean: [],
                                binary: [],
                                text: [],
                                keyword: [
                                    'iso',
                                    'adm1',
                                    'ifl',
                                    'tcs',
                                    'global_land_cover',
                                    'wdpa',
                                    'plantations',
                                    'water_stress',
                                    'primary_forest',
                                    'aze',
                                    'tiger_cl',
                                    'landmark',
                                    'land_right',
                                    'kba',
                                    'mining',
                                    'oil_palm',
                                    'idn_forest_moratorium',
                                    'wood_fiber',
                                    'resource_right',
                                    'managed_forests'
                                ]
                            },
                            clonedHost: {},
                            errorMessage: '',
                            taskId: '/v1/doc-importer/task/4d3da319-eb8a-461f-80c2-21831515e352',
                            createdAt: '2019-11-27T10:32:19.483Z',
                            updatedAt: '2019-12-05T14:39:07.966Z',
                            dataLastUpdated: null,
                            widgetRelevantProps: [],
                            layerRelevantProps: []
                        }
                    }
                }
            });

        response.status.should.equal(200);
        response.body.should.have.property('tableName').and.equal('index_ef7d64c631664053a0b7e221d84496a5_1575556546107');
        response.body.should.have.property('fields').and.eql(fieldsStructure);
    });

    afterEach(async () => {
        await channel.assertQueue(config.get('queues.tasks'));
        await channel.purgeQueue(config.get('queues.tasks'));
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
    });

    after(async () => {
        rabbitmqConnection.close();
        process.removeListener('unhandledRejection', should.fail);
    });
});

const nock = require('nock');
const deepEqualInAnyOrder = require('deep-equal-in-any-order');
const RabbitMQConnectionError = require('errors/rabbitmq-connection.error');
const chai = require('chai');
const amqp = require('amqplib');
const config = require('config');
const sleep = require('sleep');
const { getTestServer } = require('./utils/test-server');
const { createMockGetDataset } = require('./utils/helpers');

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

        nock(elasticUri)
            .get('/index_d1ced4227cd5480a8904d3410d75bf42_1587619728489/_mapping')
            .reply(200, {
                index_d1ced4227cd5480a8904d3410d75bf42_1587619728489: {
                    mappings: {
                        properties: fieldsStructure
                    }
                }
            });

        const timestamp = new Date().getTime();

        createMockGetDataset(timestamp);

        const response = await requester
            .post(`/api/v1/document/fields/${timestamp}`)
            .send();

        response.status.should.equal(200);
        response.body.should.have.property('tableName').and.equal('index_d1ced4227cd5480a8904d3410d75bf42_1587619728489');
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

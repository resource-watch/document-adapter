const nock = require('nock');
const deepEqualInAnyOrder = require('deep-equal-in-any-order');
const RabbitMQConnectionError = require('errors/rabbitmq-connection.error');
const chai = require('chai');
const amqp = require('amqplib');
const config = require('config');
const sleep = require('sleep');
const { getTestServer } = require('./utils/test-server');
const { createMockGetDataset, createIndex, deleteTestIndeces } = require('./utils/helpers');

const should = chai.should();
chai.use(deepEqualInAnyOrder);

const requester = getTestServer();
let rabbitmqConnection = null;
let channel;

nock.disableNetConnect();
nock.enableNetConnect((host) => [`${process.env.HOST_IP}:${process.env.PORT}`, process.env.ELASTIC_TEST_URL].includes(host));

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
    });

    beforeEach(async () => {
        channel = await rabbitmqConnection.createConfirmChannel();

        await channel.assertQueue(config.get('queues.tasks'));
        await channel.purgeQueue(config.get('queues.tasks'));

        const tasksQueueStatus = await channel.checkQueue(config.get('queues.tasks'));
        tasksQueueStatus.messageCount.should.equal(0);
    });

    it('Getting the fields for a dataset without connectorType document should fail', async () => {
        const datasetId = new Date().getTime();

        createMockGetDataset(datasetId, { connectorType: 'foo' });

        const requestBody = {
            loggedUser: null
        };

        const queryResponse = await requester
            .post(`/api/v1/document/fields/${datasetId}`)
            .send(requestBody);

        queryResponse.status.should.equal(422);
        queryResponse.body.should.have.property('errors').and.be.an('array').and.have.lengthOf(1);
        queryResponse.body.errors[0].detail.should.include('This operation is only supported for datasets with connectorType \'document\'');
    });

    it('Getting the fields for a dataset without a supported provider should fail', async () => {
        const datasetId = new Date().getTime();

        createMockGetDataset(datasetId, { provider: 'foo' });

        const requestBody = {
            loggedUser: null
        };

        const queryResponse = await requester
            .post(`/api/v1/document/fields/${datasetId}`)
            .send(requestBody);

        queryResponse.status.should.equal(422);
        queryResponse.body.should.have.property('errors').and.be.an('array').and.have.lengthOf(1);
        queryResponse.body.errors[0].detail.should.include('This operation is only supported for datasets with provider [\'json\', \'csv\', \'tsv\', \'xml\']');
    });

    it('Getting the fields for a dataset should return a 200 (happy case)', async () => {
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

        await createIndex('test_index_d1ced4227cd5480a8904d3410d75bf42_1587619728489', fieldsStructure);

        const datasetId = new Date().getTime();

        createMockGetDataset(datasetId);

        const response = await requester
            .post(`/api/v1/document/fields/${datasetId}`)
            .send();

        response.status.should.equal(200);
        response.body.should.have.property('tableName').and.equal('test_index_d1ced4227cd5480a8904d3410d75bf42_1587619728489');
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
        await deleteTestIndeces();

        rabbitmqConnection.close();
        process.removeListener('unhandledRejection', should.fail);
    });
});

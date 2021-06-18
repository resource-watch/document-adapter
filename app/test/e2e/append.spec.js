const nock = require('nock');
const deepEqualInAnyOrder = require('deep-equal-in-any-order');
const RabbitMQConnectionError = require('errors/rabbitmq-connection.error');
const chai = require('chai');
const amqp = require('amqplib');
const uuid = require('uuid');
const config = require('config');
const sleep = require('sleep');
const { task } = require('rw-doc-importer-messages');
const { getTestServer } = require('./utils/test-server');
const { USERS } = require('./utils/test.constants');
const { createMockGetDataset } = require('./utils/helpers');

chai.should();
chai.use(deepEqualInAnyOrder);

let requester;
let rabbitmqConnection = null;
let channel;

nock.disableNetConnect();
nock.enableNetConnect((host) => [`${process.env.HOST_IP}:${process.env.PORT}`, process.env.ELASTIC_TEST_URL].includes(host));

describe('Dataset append tests', () => {

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

    it('Append a dataset without user should return an error', async () => {
        const datasetId = uuid.v4();

        const postBody = {
            data: [{ data: 'value' }],
            dataPath: 'new data path',
            legend: 'new legend',
            url: 'https://wri-01.carto.com/tables/wdpa_protected_areas/table-new.csv',
            provider: 'csv'
        };
        const response = await requester
            .post(`/api/v1/document/${datasetId}/append`)
            .send(postBody);

        response.status.should.equal(401);
        response.body.should.have.property('errors').and.be.an('array');
        response.body.errors[0].should.have.property('detail').and.equal(`Unauthorized`);
    });

    it('Append a dataset without a valid dataset should return a 400 error', async () => {
        const datasetId = uuid.v4();

        nock(process.env.GATEWAY_URL)
            .get(`/v1/dataset/${datasetId}`)
            .reply(404, {
                errors: [
                    {
                        status: 404,
                        detail: `Dataset with id '${datasetId}' doesn't exist`
                    }
                ]
            });

        const postBody = {
            data: [{ data: 'value' }],
            dataPath: 'new data path',
            legend: 'new legend',
            url: 'https://wri-01.carto.com/tables/wdpa_protected_areas/table-new.csv',
            provider: 'csv',
            loggedUser: USERS.ADMIN
        };

        const response = await requester
            .post(`/api/v1/document/${datasetId}/append`)
            .send(postBody);

        response.status.should.equal(404);
        response.body.should.have.property('errors').and.be.an('array');
        response.body.errors[0].should.have.property('detail').and.equal(`Dataset with id '${datasetId}' doesn't exist`);
    });

    it('Append a dataset for a different application should return an error', async () => {
        const datasetId = uuid.v4();

        createMockGetDataset(datasetId, { application: ['fake-app'] });

        const postBody = {
            data: [{ data: 'value' }],
            dataPath: 'new data path',
            legend: 'new legend',
            url: 'https://wri-01.carto.com/tables/wdpa_protected_areas/table-new.csv',
            provider: 'csv',
            loggedUser: USERS.ADMIN
        };

        const response = await requester
            .post(`/api/v1/document/${datasetId}/append`)
            .send(postBody);

        response.status.should.equal(403);
        response.body.should.have.property('errors').and.be.an('array');
        response.body.errors[0].should.have.property('detail').and.equal(`Not authorized`);
    });

    it('Append a dataset with an invalid type should fail', async () => {
        const datasetId = uuid.v4();

        createMockGetDataset(datasetId, { connectorType: 'carto' });

        const postBody = {
            data: [{ data: 'value' }],
            dataPath: 'new data path',
            legend: 'new legend',
            url: 'https://wri-01.carto.com/tables/wdpa_protected_areas/table-new.csv',
            provider: 'csv',
            loggedUser: USERS.ADMIN
        };

        const response = await requester
            .post(`/api/v1/document/${datasetId}/append`)
            .send(postBody);

        response.status.should.equal(422);
        response.body.should.have.property('errors').and.be.an('array');
        response.body.errors[0].should.have.property('detail').and.equal(`This operation is only supported for datasets with connectorType 'document'`);
    });

    it('Append a CSV dataset with data POST body should be successful (happy case)', async () => {
        const datasetId = uuid.v4();

        createMockGetDataset(datasetId);

        const postBody = {
            data: [{ data: 'value' }],
            dataPath: 'new data path',
            provider: 'csv',
            loggedUser: USERS.ADMIN
        };
        const response = await requester
            .post(`/api/v1/document/${datasetId}/append`)
            .send(postBody);

        response.status.should.equal(200);

        let expectedStatusQueueMessageCount = 1;

        const validateStatusQueueMessages = (resolve) => async (msg) => {
            const content = JSON.parse(msg.content.toString());

            content.should.have.property('type').and.equal(task.MESSAGE_TYPES.TASK_APPEND);
            content.should.have.property('data').and.equalInAnyOrder(postBody.data);
            content.should.have.property('dataPath').and.equal(postBody.dataPath);
            content.should.have.property('datasetId').and.equal(`${datasetId}`);
            content.should.have.property('provider').and.equal('csv');
            content.should.have.property('id');
            content.should.have.property('index').and.equal('test_index_d1ced4227cd5480a8904d3410d75bf42_1587619728489');
            content.should.have.property('legend').and.deep.equal({});
            content.should.have.property('provider').and.equal('csv');

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

    it('Append a CSV dataset with data from URL/file using the \'url\' deprecated field should be successful (happy case)', async () => {
        const datasetId = uuid.v4();

        createMockGetDataset(datasetId);

        const postBody = {
            url: 'http://gfw2-data.s3.amazonaws.com/country-pages/umd_landsat_alerts_adm2_staging.csv',
            provider: 'csv',
            loggedUser: USERS.ADMIN
        };
        const response = await requester
            .post(`/api/v1/document/${datasetId}/append`)
            .send(postBody);

        response.status.should.equal(200);

        let expectedStatusQueueMessageCount = 1;

        const validateStatusQueueMessages = (resolve) => async (msg) => {
            const content = JSON.parse(msg.content.toString());

            content.should.have.property('datasetId').and.equal(`${datasetId}`);
            content.should.have.property('provider').and.equal('csv');
            content.should.have.property('fileUrl').and.be.an('array').and.eql([postBody.url]);
            content.should.have.property('id');
            content.should.have.property('index').and.equal('test_index_d1ced4227cd5480a8904d3410d75bf42_1587619728489');
            content.should.have.property('provider').and.equal('csv');
            content.should.have.property('type').and.equal(task.MESSAGE_TYPES.TASK_APPEND);

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

    it('Append a CSV dataset with data from URL/file using the \'sources\' field should be successful (happy case)', async () => {
        const datasetId = uuid.v4();

        createMockGetDataset(datasetId);

        const postBody = {
            sources: ['http://gfw2-data.s3.amazonaws.com/country-pages/umd_landsat_alerts_adm2_staging.csv'],
            provider: 'csv',
            loggedUser: USERS.ADMIN
        };
        const response = await requester
            .post(`/api/v1/document/${datasetId}/append`)
            .send(postBody);

        response.status.should.equal(200);

        let expectedStatusQueueMessageCount = 1;

        const validateStatusQueueMessages = (resolve) => async (msg) => {
            const content = JSON.parse(msg.content.toString());

            content.should.have.property('datasetId').and.equal(`${datasetId}`);
            content.should.have.property('provider').and.equal('csv');
            content.should.have.property('fileUrl').and.be.an('array').and.eql(postBody.sources);
            content.should.have.property('id');
            content.should.have.property('index').and.equal('test_index_d1ced4227cd5480a8904d3410d75bf42_1587619728489');
            content.should.have.property('provider').and.equal('csv');
            content.should.have.property('type').and.equal(task.MESSAGE_TYPES.TASK_APPEND);

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

    it('Append a CSV dataset with data from multiple files should be successful (happy case)', async () => {
        const datasetId = uuid.v4();

        createMockGetDataset(datasetId);

        const postBody = {
            sources: [
                'http://api.resourcewatch.org/v1/dataset?page[number]=1&page[size]=10',
                'http://api.resourcewatch.org/v1/dataset?page[number]=2&page[size]=10',
                'http://api.resourcewatch.org/v1/dataset?page[number]=3&page[size]=10'
            ],
            provider: 'csv',
            loggedUser: USERS.ADMIN
        };

        const response = await requester
            .post(`/api/v1/document/${datasetId}/append`)
            .send(postBody);

        response.status.should.equal(200);

        let expectedStatusQueueMessageCount = 1;

        const validateStatusQueueMessages = (resolve) => async (msg) => {
            const content = JSON.parse(msg.content.toString());

            content.should.have.property('datasetId').and.equal(`${datasetId}`);
            content.should.have.property('provider').and.equal('csv');
            content.should.have.property('fileUrl').and.be.an('array').and.eql(postBody.sources);
            content.should.have.property('id');
            content.should.have.property('index').and.equal('test_index_d1ced4227cd5480a8904d3410d75bf42_1587619728489');
            content.should.have.property('provider').and.equal('csv');
            content.should.have.property('type').and.equal(task.MESSAGE_TYPES.TASK_APPEND);

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

    it('Append a CSV dataset with append=true should be successful (param is ignored)', async () => {
        const datasetId = uuid.v4();

        createMockGetDataset(datasetId, { append: true });

        const postBody = {
            sources: ['http://gfw2-data.s3.amazonaws.com/country-pages/umd_landsat_alerts_adm2_staging.csv'],
            provider: 'csv',
            loggedUser: USERS.ADMIN
        };
        const response = await requester
            .post(`/api/v1/document/${datasetId}/append?append=true`)
            .send(postBody);

        response.status.should.equal(200);

        let expectedStatusQueueMessageCount = 1;

        const validateStatusQueueMessages = (resolve) => async (msg) => {
            const content = JSON.parse(msg.content.toString());

            content.should.have.property('datasetId').and.equal(`${datasetId}`);
            content.should.have.property('provider').and.equal('csv');
            content.should.have.property('fileUrl').and.be.an('array').and.eql(postBody.sources);
            content.should.have.property('id');
            content.should.have.property('index').and.equal('test_index_d1ced4227cd5480a8904d3410d75bf42_1587619728489');
            content.should.have.property('provider').and.equal('csv');
            content.should.have.property('type').and.equal(task.MESSAGE_TYPES.TASK_APPEND);

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
            throw new Error(`Not all nock interceptors were used: ${pendingMocks}`);
        }

        await channel.close();
        channel = null;

        await rabbitmqConnection.close();
        rabbitmqConnection = null;
    });
});

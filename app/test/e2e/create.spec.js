const nock = require('nock');
const chai = require('chai');
const amqp = require('amqplib');
const uuid = require('uuid');
const config = require('config');
const { task } = require('rw-doc-importer-messages');
const sleep = require('sleep');
const RabbitMQConnectionError = require('errors/rabbitmq-connection.error');
const { getTestServer } = require('./utils/test-server');
const { ROLES } = require('./utils/test.constants');

chai.should();

let requester;
let rabbitmqConnection = null;
let channel;

nock.disableNetConnect();
nock.enableNetConnect((host) => [`${process.env.HOST_IP}:${process.env.PORT}`, process.env.ELASTIC_TEST_URL].includes(host));

describe('Dataset create tests', () => {

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

    /* Create a CSV Dataset */
    it('Create a CSV dataset should be successful (happy case)', async () => {
        const preQueueStatus = await channel.assertQueue(config.get('queues.tasks'));
        preQueueStatus.messageCount.should.equal(0);

        const datasetId = uuid.v4();
        const connector = {
            id: datasetId,
            name: `Carto DB Dataset - ${datasetId}`,
            application: ['rw'],
            connectorType: 'rest',
            env: 'production',
            sources: ['https://wri-01.carto.com/tables/wdpa_protected_areas/table.csv'],
            overwrite: true
        };
        const response = await requester
            .post(`/api/v1/document/csv`)
            .send({
                connector,
                loggedUser: ROLES.ADMIN
            });

        response.status.should.equal(200);

        let expectedStatusQueueMessageCount = 1;

        const validateStatusQueueMessages = (resolve) => async (msg) => {
            const content = JSON.parse(msg.content.toString());

            content.should.have.property('datasetId').and.equal(connector.id);
            content.should.have.property('fileUrl').and.be.an('array').and.eql(connector.sources);
            content.should.have.property('provider').and.equal('csv');
            content.should.have.property('type').and.equal(task.MESSAGE_TYPES.TASK_CREATE);

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

    it('Create a JSON dataset from a single file should be successful (happy case)', async () => {
        const preQueueStatus = await channel.assertQueue(config.get('queues.tasks'));
        preQueueStatus.messageCount.should.equal(0);

        const datasetId = uuid.v4();
        const connector = {
            id: datasetId,
            name: `JSON Dataset - ${datasetId}`,
            application: ['rw'],
            connectorType: 'rest',
            env: 'production',
            sources: ['https://wri-01.carto.com/tables/wdpa_protected_areas/table.json'],
            overwrite: true
        };
        const response = await requester
            .post(`/api/v1/document/json`)
            .send({
                connector,
                loggedUser: ROLES.ADMIN
            });

        response.status.should.equal(200);

        let expectedStatusQueueMessageCount = 1;

        const validateStatusQueueMessages = (resolve) => async (msg) => {
            const content = JSON.parse(msg.content.toString());

            content.should.have.property('datasetId').and.equal(connector.id);
            content.should.have.property('fileUrl').and.be.an('array').and.eql(connector.sources);
            content.should.have.property('provider').and.equal('json');
            content.should.have.property('type').and.equal(task.MESSAGE_TYPES.TASK_CREATE);

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

    it('Create a JSON dataset from multiple files should be successful (happy case)', async () => {
        const preQueueStatus = await channel.assertQueue(config.get('queues.tasks'));
        preQueueStatus.messageCount.should.equal(0);

        const datasetId = uuid.v4();
        const connector = {
            id: datasetId,
            name: `Carto DB Dataset - ${datasetId}`,
            application: ['rw'],
            connectorType: 'rest',
            env: 'production',
            connectorUrl: null,
            sources: [
                'http://api.resourcewatch.org/v1/dataset?page[number]=1&page[size]=10',
                'http://api.resourcewatch.org/v1/dataset?page[number]=2&page[size]=10',
                'http://api.resourcewatch.org/v1/dataset?page[number]=3&page[size]=10'
            ],
            overwrite: true
        };
        const response = await requester
            .post(`/api/v1/document/json`)
            .send({
                connector,
                loggedUser: ROLES.ADMIN
            });

        response.status.should.equal(200);

        let expectedStatusQueueMessageCount = 1;

        const validateStatusQueueMessages = (resolve) => async (msg) => {
            const content = JSON.parse(msg.content.toString());

            content.should.have.property('datasetId').and.equal(connector.id);
            content.should.have.property('fileUrl').and.be.an('array').and.eql(connector.sources);
            content.should.have.property('provider').and.equal('json');
            content.should.have.property('type').and.equal(task.MESSAGE_TYPES.TASK_CREATE);

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

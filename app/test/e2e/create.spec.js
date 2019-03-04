/* eslint-disable no-unused-vars,no-undef */
const nock = require('nock');
const chai = require('chai');
const amqp = require('amqplib');
const config = require('config');
const { task } = require('rw-doc-importer-messages');
const { getTestServer } = require('./test-server');
const { ROLES } = require('./test.constants');
const sleep = require('sleep');

const should = chai.should();

const requester = getTestServer();

nock.disableNetConnect();
nock.enableNetConnect(`${process.env.HOST_IP}:${process.env.PORT}`);

describe('Dataset create tests', () => {

    before(async () => {
        if (process.env.NODE_ENV !== 'test') {
            throw Error(`Running the test suite with NODE_ENV ${process.env.NODE_ENV} may result in permanent data loss. Please use NODE_ENV=test.`);
        }

        nock.cleanAll();
    });

    /* Create a CSV Dataset */
    it('Create a CSV dataset should be successful (happy case)', async () => {
        const queueName = config.get('queues.docTasks');
        const conn = await amqp.connect(config.get('rabbitmq.url'));
        const channel = await conn.createConfirmChannel();
        await channel.assertQueue(queueName);
        await channel.purgeQueue(queueName);

        const preQueueStatus = await channel.assertQueue(queueName);
        preQueueStatus.messageCount.should.equal(0);

        const timestamp = new Date().getTime();
        const connector = {
            id: timestamp,
            name: `Carto DB Dataset - ${timestamp}`,
            application: ['rw'],
            connectorType: 'rest',
            env: 'production',
            connectorUrl: 'https://wri-01.carto.com/tables/wdpa_protected_areas/table.csv',
            overwrite: true
        };
        const response = await requester
            .post(`/api/v1/document/csv`)
            .send({
                connector,
                loggedUser: ROLES.ADMIN
            });

        response.status.should.equal(200);

        sleep.sleep(2);

        const postQueueStatus = await channel.assertQueue(queueName);
        postQueueStatus.messageCount.should.equal(1);

        const validateMessage = (msg) => {
            const content = JSON.parse(msg.content.toString());
            content.should.have.property('datasetId').and.equal(connector.id);
            content.should.have.property('fileUrl').and.equal(connector.connectorUrl);
            content.should.have.property('provider').and.equal('csv');
            content.should.have.property('type').and.equal(task.MESSAGE_TYPES.TASK_CREATE);
        };

        await channel.consume(queueName, validateMessage);

        await channel.purgeQueue(queueName);
        conn.close();
    });

    /* Create a JSON Dataset */
    it('Create a JSON dataset should be successful (happy case)', async () => {
        const queueName = config.get('queues.docTasks');
        const conn = await amqp.connect(config.get('rabbitmq.url'));
        const channel = await conn.createConfirmChannel();
        await channel.purgeQueue(queueName);

        const preQueueStatus = await channel.assertQueue(queueName);
        preQueueStatus.messageCount.should.equal(0);

        const timestamp = new Date().getTime();
        const connector = {
            id: timestamp,
            name: `Carto DB Dataset - ${timestamp}`,
            application: ['rw'],
            connectorType: 'rest',
            env: 'production',
            connectorUrl: 'https://wri-01.carto.com/tables/wdpa_protected_areas/table.json',
            overwrite: true
        };
        const response = await requester
            .post(`/api/v1/document/json`)
            .send({
                connector,
                loggedUser: ROLES.ADMIN
            });

        response.status.should.equal(200);

        sleep.sleep(2);

        const postQueueStatus = await channel.assertQueue(queueName);
        postQueueStatus.messageCount.should.equal(1);

        const validateMessage = (msg) => {
            const content = JSON.parse(msg.content.toString());
            content.should.have.property('datasetId').and.equal(connector.id);
            content.should.have.property('fileUrl').and.equal(connector.connectorUrl);
            content.should.have.property('provider').and.equal('json');
            content.should.have.property('type').and.equal(task.MESSAGE_TYPES.TASK_CREATE);
        };

        await channel.consume(queueName, validateMessage);

        await channel.purgeQueue(queueName);
        conn.close();
    });

    afterEach(() => {
        if (!nock.isDone()) {
            throw new Error(`Not all nock interceptors were used: ${nock.pendingMocks()}`);
        }
    });

    after(async () => {
        const conn = await amqp.connect(config.get('rabbitmq.url'));
        const channel = await conn.createConfirmChannel();
        await channel.purgeQueue(config.get('queues.docTasks'));
    });
});

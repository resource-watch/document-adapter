/* eslint-disable no-unused-vars,no-undef */
const nock = require('nock');
const deepEqualInAnyOrder = require('deep-equal-in-any-order');
const chai = require('chai');
const amqp = require('amqplib');
const config = require('config');
const { task } = require('doc-importer-messages');
const { getTestServer } = require('./test-server');
const { ROLES } = require('./test.constants');

const should = chai.should();
chai.use(deepEqualInAnyOrder);

const requester = getTestServer();

nock.disableNetConnect();
nock.enableNetConnect(`${process.env.HOST_IP}:${process.env.PORT}`);

describe('Dataset overwrite tests', () => {

    before(async () => {
        if (process.env.NODE_ENV !== 'test') {
            throw Error(`Running the test suite with NODE_ENV ${process.env.NODE_ENV} may result in permanent data loss. Please use NODE_ENV=test.`);
        }

        nock.cleanAll();
    });

    it('Overwrite a CSV dataset should be successful (happy case)', async () => {
        const queueName = config.get('queues.docTasks');
        const conn = await amqp.connect(config.get('rabbitmq.url'));
        const channel = await conn.createConfirmChannel();
        await channel.assertQueue(queueName);
        await channel.purgeQueue(queueName);

        const preQueueStatus = await channel.assertQueue(queueName);
        preQueueStatus.messageCount.should.equal(0);

        const timestamp = new Date().getTime();
        const dataset = {
            userId: 1,
            application: ['rw'],
            overwrite: true,
            status: 'saved',
            tableName: 'new-table-name'
        };
        const postBody = {
            dataset,
            data: [{ data: 'value' }],
            dataPath: 'new data path',
            legend: 'new legend',
            url: 'https://wri-01.carto.com/tables/wdpa_protected_areas/table-new.csv',
            provider: 'csv',
            loggedUser: ROLES.ADMIN
        };
        const response = await requester
            .post(`/api/v1/document/${timestamp}/data-overwrite`)
            .send(postBody);

        response.status.should.equal(200);

        const postQueueStatus = await channel.assertQueue(queueName);
        postQueueStatus.messageCount.should.equal(1);

        const validateMessage = (msg) => {
            const content = JSON.parse(msg.content.toString());
            content.should.have.property('data').and.equalInAnyOrder(postBody.data);
            content.should.have.property('dataPath').and.equal(postBody.dataPath);
            content.should.have.property('datasetId').and.equal(`${timestamp}`);
            content.should.have.property('provider').and.equal('csv');
            content.should.have.property('fileUrl').and.equal(postBody.url);
            content.should.have.property('id');
            content.should.have.property('index').and.equal(dataset.tableName);
            content.should.have.property('legend').and.equal(postBody.legend);
            content.should.have.property('provider').and.equal('csv');
            content.should.have.property('type').and.equal(task.MESSAGE_TYPES.TASK_OVERWRITE);
        };

        await channel.consume(queueName, validateMessage.bind(this));

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
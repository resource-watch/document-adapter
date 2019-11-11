/* eslint-disable no-unused-vars,no-undef,no-await-in-loop */
const nock = require('nock');
const deepEqualInAnyOrder = require('deep-equal-in-any-order');
const RabbitMQConnectionError = require('errors/rabbitmq-connection.error');
const chai = require('chai');
const amqp = require('amqplib');
const config = require('config');
const sleep = require('sleep');
const { task } = require('rw-doc-importer-messages');
const { getTestServer } = require('./test-server');
const { ROLES } = require('./test.constants');

const should = chai.should();
chai.use(deepEqualInAnyOrder);

const requester = getTestServer();
let rabbitmqConnection = null;
let channel;

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
    });

    beforeEach(async () => {
        channel = await rabbitmqConnection.createConfirmChannel();

        await channel.assertQueue(config.get('queues.tasks'));
        await channel.purgeQueue(config.get('queues.tasks'));

        const tasksQueueStatus = await channel.checkQueue(config.get('queues.tasks'));
        tasksQueueStatus.messageCount.should.equal(0);
    });

    it('Delete dataset index with tableName null should nothing to do and return success (happy case)', async () => {
        const timestamp = new Date().getTime();

        nock(process.env.CT_URL).get(`/v1/dataset/${timestamp}`).reply(200, {
            data: {
                attributes: {
                    tableName: null
                }
            }
        });

        const response = await requester.delete(`/api/v1/document/${timestamp}`);
        response.status.should.equal(200);

        await new Promise(resolve => setTimeout(resolve, 3000));

        const postQueueStatus = await channel.assertQueue(config.get('queues.tasks'));
        postQueueStatus.messageCount.should.equal(0);
    });

    it('Delete dataset index should be successful (happy case)', async () => {
        const timestamp = new Date().getTime();

        nock(process.env.CT_URL).get(`/v1/dataset/${timestamp}`).reply(200, {
            data: {
                attributes: {
                    tableName: 'test'
                }
            }
        });

        const response = await requester.delete(`/api/v1/document/${timestamp}`);
        response.status.should.equal(200);

        await new Promise(resolve => setTimeout(resolve, 3000));

        const postQueueStatus = await channel.assertQueue(config.get('queues.tasks'));
        postQueueStatus.messageCount.should.equal(1);

        const validateMessage = async (msg) => {
            const content = JSON.parse(msg.content.toString());
            content.should.have.property('index').and.equal('test');
            content.should.have.property('datasetId').and.equal(`${timestamp}`);

            await channel.ack(msg);
        };

        await channel.consume(config.get('queues.tasks'), validateMessage.bind(this));

        process.on('unhandledRejection', (error) => {
            should.fail(error);
        });
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
    });
});

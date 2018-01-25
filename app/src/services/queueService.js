const logger = require('logger');
const config = require('config');
const amqp = require('amqplib');
const co = require('co');

class QueueService {

    constructor(q, consume = false) {
        this.q = q;
        logger.debug(`Connecting to queue ${this.q}`);
        try {
            this.init(consume).then(() => {
                logger.debug('Connected');
            }, (err) => {
                logger.error(err);
                process.exit(1);
            });
        } catch (err) {
            logger.error(err);
            process.exit(1);
        }
    }

    init(consume) {
        return co(function* () {
            const conn = yield amqp.connect(config.get('rabbitmq.url'));
            this.channel = yield conn.createConfirmChannel();
            yield this.channel.assertQueue(this.q, { durable: true });
            if (consume) {
                this.channel.prefetch(1);
                logger.debug(` [*] Waiting for messages in ${this.q}`);
                this.channel.consume(this.q, this.consume.bind(this), {
                    noAck: false
                });
            }
        }.bind(this));

    }

    returnMsg(msg) {
        logger.debug(`Sending message to ${this.q}`);
        try {
            // Sending to queue
            let count = msg.properties.headers['x-redelivered-count'] || 0;
            count += 1;
            this.channel.sendToQueue(this.q, msg.content, { headers: { 'x-redelivered-count': count } });
        } catch (err) {
            logger.error(`Error sending message to ${this.q}`);
            throw err;
        }
    }

    consume() {

    }

}

module.exports = QueueService;

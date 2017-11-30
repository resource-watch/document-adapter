const logger = require('logger');
const QueueService = require('services/queueService');
const { TASKS_QUEUE } = require('appConstants');
const { task } = require('doc-importer-messages');

class TaskQueueService extends QueueService {

    constructor() {
        super(TASKS_QUEUE);
    }

    sendMessage(msg) {
        return (cb) => {
            logger.info(`Sending message to ${this.q}`, msg);
            try {
                // Sending to queue
                this.channel.sendToQueue(this.q, Buffer.from(JSON.stringify(msg)));
                cb(null);
            } catch (err) {
                logger.error(`Error sending message to ${this.q}`);
                cb(err);
            }
        }
    }

    * import(data) {
        const taskMessage = task.createMessage(task.MESSAGE_TYPES.TASK_CREATE, data);
        yield this.sendMessage(taskMessage);
    }

    * overwrite(data) {
        const taskMessage = task.createMessage(task.MESSAGE_TYPES.TASK_OVERWRITE, data);
        yield this.sendMessage(taskMessage);
    }

    * concat(data) {
        const taskMessage = task.createMessage(task.MESSAGE_TYPES.TASK_CONCAT, data);
        yield this.sendMessage(taskMessage);
    }

    * delete(data) {
        const taskMessage = task.createMessage(task.MESSAGE_TYPES.TASK_DELETE, data);
        yield this.sendMessage(taskMessage);
    }

    * deleteIndex(data) {
        const taskMessage = task.createMessage(task.MESSAGE_TYPES.TASK_DELETE_INDEX, data);
        yield this.sendMessage(taskMessage);
    }

}

module.exports = new TaskQueueService();

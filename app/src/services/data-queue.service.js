const logger = require('logger');
const config = require('config');
const amqp = require('amqplib');
const docImporterMessages = require('rw-doc-importer-messages');
const StatusQueueService = require('services/status-queue.service');


class DataQueueService {

    constructor() {
        logger.info(`Connecting to queue ${config.get('queues.data')}`);
        try {
            this.init().then(() => {
                logger.info('[Data Queue] Connected');
            }, (err) => {
                logger.error(err);
                process.exit(1);
            });
        } catch (err) {
            logger.error(err);
        }
    }

    async init() {
        const conn = await amqp.connect(config.get('rabbitmq.url'));
        this.channel = await conn.createConfirmChannel();
    }

    async sendMessage(msg) {
        return new Promise((resolve) => {
            const interval = setInterval(async () => {
                try {
                    logger.info('[Data Queue] Sending message');
                    const data = await this.channel.assertQueue(config.get('queues.data'), {
                        durable: true
                    });
                    if (data.count > 100) {
                        throw new Error('Full queue');
                    }
                    this.channel.sendToQueue(config.get('queues.data'), Buffer.from(JSON.stringify(msg)));
                    clearInterval(interval);
                    resolve();
                } catch (err) {
                    logger.error('[Data Queue] Error sending message (try again in 2 second)', err);
                }
            }, 2000);
        });
    }

    async sendDataMessage(taskId, index, data) {
        logger.debug(`[Data Queue] Sending data message (${data.length})`);
        await this.sendMessage(docImporterMessages.data.createMessage(docImporterMessages.data.MESSAGE_TYPES.DATA, {
            taskId,
            index,
            data
        }));
        await StatusQueueService.sendReadData(taskId);
    }


}

module.exports = new DataQueueService();

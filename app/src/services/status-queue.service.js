const logger = require('logger');
const config = require('config');
const amqp = require('amqplib');
const docImporterMessages = require('rw-doc-importer-messages');

class StatusQueueService {

    constructor() {
        logger.info(`Connecting to queue ${config.get('queues.status')}`);
        try {
            this.init().then(() => {
                logger.info('Connected');
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
        return new Promise((resolve, reject) => {
            let numTries = 0;
            const interval = setInterval(async () => {
                try {
                    numTries += 1;
                    logger.info('[Status Queue] Sending message', msg);
                    await this.channel.assertQueue(config.get('queues.status'), {
                        durable: true
                    });
                    this.channel.sendToQueue(config.get('queues.status'), Buffer.from(JSON.stringify(msg)));
                    clearInterval(interval);
                    resolve();
                } catch (err) {
                    logger.error('[Status Queue] Error sending message (try again in 2 second)', err);
                    if (numTries > 3) {
                        clearInterval(interval);
                        reject(err);
                    }
                }
            }, 2000);
        });
    }

    async sendIndexCreated(taskId, index) {
        logger.debug('[Status Queue] Sending index created message of taskId', taskId, 'and index', index);
        await this.sendMessage(docImporterMessages.status.createMessage(docImporterMessages.status.MESSAGE_TYPES.STATUS_INDEX_CREATED, {
            taskId,
            index
        }));
    }

    async sendIndexDeactivated(taskId, index) {
        logger.debug('[Status Queue] Sending index deactivated message of taskId', taskId, 'and index', index);
        await this.sendMessage(docImporterMessages.status.createMessage(docImporterMessages.status.MESSAGE_TYPES.STATUS_INDEX_DEACTIVATED, {
            taskId,
            index
        }));
    }

    async sendBlockChainGenerated(taskId, blockchain) {
        logger.debug('[Status Queue] Sending Blockchain generated of taskId', taskId);
        await this.sendMessage(docImporterMessages.status.createMessage(docImporterMessages.status.MESSAGE_TYPES.STATUS_BLOCKCHAIN_GENERATED, {
            taskId,
            blockchain
        }));
    }

    async sendReadData(taskId) {
        logger.debug('[Status Queue] Sending Read data of taskId', taskId);
        await this.sendMessage(docImporterMessages.status.createMessage(docImporterMessages.status.MESSAGE_TYPES.STATUS_READ_DATA, {
            taskId
        }));
    }

    async sendReadFile(taskId) {
        logger.debug('[Status Queue] Sending Read File of taskId', taskId);
        await this.sendMessage(docImporterMessages.status.createMessage(docImporterMessages.status.MESSAGE_TYPES.STATUS_READ_FILE, {
            taskId
        }));
    }

    async sendImportConfirmed(taskId) {
        logger.debug('[Status Queue] Sending Read File of taskId', taskId);
        await this.sendMessage(docImporterMessages.status.createMessage(docImporterMessages.status.MESSAGE_TYPES.STATUS_IMPORT_CONFIRMED, {
            taskId
        }));
    }

    async sendIndexDeleted(taskId) {
        logger.debug('[Status Queue] Sending index deleted of taskId', taskId);
        await this.sendMessage(docImporterMessages.status.createMessage(docImporterMessages.status.MESSAGE_TYPES.STATUS_INDEX_DELETED, {
            taskId
        }));
    }

    async sendPerformedDeleteQuery(taskId, elasticTaskId) {
        logger.debug('[Status Queue] Sending Perform delete query of taskId', taskId);
        await this.sendMessage(docImporterMessages.status.createMessage(docImporterMessages.status.MESSAGE_TYPES.STATUS_PERFORMED_DELETE_QUERY, {
            taskId,
            elasticTaskId
        }));
    }

    async sendPerformedReindex(taskId, elasticTaskId) {
        logger.debug('[Status Queue] Sending Perform reindex of taskId', taskId);
        await this.sendMessage(docImporterMessages.status.createMessage(docImporterMessages.status.MESSAGE_TYPES.STATUS_PERFORMED_REINDEX, {
            taskId,
            elasticTaskId
        }));
    }

    async sendFinishedDeleteQuery(taskId) {
        logger.debug('[Status Queue] Sending finished delete query of taskId', taskId);
        await this.sendMessage(docImporterMessages.status.createMessage(docImporterMessages.status.MESSAGE_TYPES.STATUS_FINISHED_DELETE_QUERY, {
            taskId
        }));
    }

    async sendFinishedReindex(taskId) {
        logger.debug('[Status Queue] Sending finished reindex of taskId', taskId);
        await this.sendMessage(docImporterMessages.status.createMessage(docImporterMessages.status.MESSAGE_TYPES.STATUS_FINISHED_REINDEX, {
            taskId
        }));
    }

    async sendErrorMessage(taskId, error) {
        logger.debug('[Status Queue] Sending error message of taskId', taskId);
        await this.sendMessage(docImporterMessages.status.createMessage(docImporterMessages.status.MESSAGE_TYPES.STATUS_ERROR, {
            taskId,
            error
        }));
    }

}

module.exports = new StatusQueueService();

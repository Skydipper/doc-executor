const logger = require('logger');
const statusQueueService = require('services/status-queue.service');
const { execution } = require('rw-doc-importer-messages');
const ImporterService = require('services/importer.service');
const elasticService = require('services/elastic.service');
const UrlNotFound = require('errors/urlNotFound');
const ElasticError = require('errors/elastic.error');

const ExecutionMessages = execution.MESSAGE_TYPES;

class ExecutorService {

    static async processMessage(msg) {
        // logger.debug('Processing message', msg);
        switch (msg.type) {

            case ExecutionMessages.EXECUTION_CREATE:
                await ExecutorService.create(msg);
                break;

            case ExecutionMessages.EXECUTION_CONCAT:
                await ExecutorService.concat(msg);
                break;

            case ExecutionMessages.EXECUTION_APPEND:
                await ExecutorService.append(msg);
                break;

            case ExecutionMessages.EXECUTION_DELETE:
                await ExecutorService.deleteQuery(msg);
                break;

            case ExecutionMessages.EXECUTION_CONFIRM_DELETE:
                await ExecutorService.confirmDelete(msg);
                break;

            case ExecutionMessages.EXECUTION_DELETE_INDEX:
                await ExecutorService.deleteIndex(msg);
                break;

            case ExecutionMessages.EXECUTION_CONFIRM_IMPORT:
                await ExecutorService.confirmImport(msg);
                break;

            case ExecutionMessages.EXECUTION_CONFIRM_REINDEX:
                await ExecutorService.confirmReIndex(msg);
                break;

            case ExecutionMessages.EXECUTION_REINDEX:
                await ExecutorService.reindex(msg);
                break;

            default:
                logger.error('Message not supported');

        }
    }

    static async create(msg) {
        // Create the index
        logger.debug('Create task');
        logger.debug('Creating index');
        const index = `index_${msg.datasetId.replace(/-/g, '')}_${Date.now()}`;
        await elasticService.createIndex(index, 'type', msg.legend);
        await elasticService.deactivateIndex(index);
        msg.indexType = 'type';
        msg.index = index;
        // Now send a STATUS_INDEX_CREATED to StatusQueue
        await statusQueueService.sendIndexCreated(msg.taskId, index);
        logger.debug('Starting importing service');
        try {
            const importerService = new ImporterService(msg);
            await importerService.start();
            logger.debug('Sending read file message');
            await statusQueueService.sendReadFile(msg.taskId);
        } catch (err) {
            if (err instanceof UrlNotFound) {
                await statusQueueService.sendErrorMessage(msg.taskId, err.message);
                return;
            }
            throw err;
        }
    }

    static async concat(msg) {
        // The Index is already created when concatenating
        logger.debug('Starting importing service');
        logger.debug('Creating index');
        const index = `index_${msg.datasetId.replace(/-/g, '')}_${Date.now()}`;
        await elasticService.createIndex(index, 'type', msg.legend);
        await elasticService.deactivateIndex(index);
        msg.indexType = 'type';
        msg.index = index;

        // Now send a STATUS_INDEX_CREATED to StatusQueue
        await statusQueueService.sendIndexCreated(msg.taskId, index);
        logger.debug('Starting importing service');
        try {
            const importerService = new ImporterService(msg);
            await importerService.start();
            logger.debug('Sending read file message');
            await statusQueueService.sendReadFile(msg.taskId);
        } catch (err) {
            if (err instanceof UrlNotFound) {
                await statusQueueService.sendErrorMessage(msg.taskId, err.message);
                return;
            }
            throw err;
        }
    }

    static async append(msg) {
        // The Index is already created when concatenating
        logger.debug('Starting append workflow');

        const { index } = msg;

        logger.debug(`Deactivating index ${index}`);

        await elasticService.deactivateIndex(index);
        msg.indexType = 'type';

        // Now send a STATUS_INDEX_DEACTIVATED to StatusQueue
        await statusQueueService.sendIndexDeactivated(msg.taskId, index);
        logger.debug('Starting importing service');
        try {
            const importerService = new ImporterService(msg);
            await importerService.start();
            logger.debug('Sending read file message');
            await statusQueueService.sendReadFile(msg.taskId);
        } catch (err) {
            if (err instanceof UrlNotFound) {
                await statusQueueService.sendErrorMessage(msg.taskId, err.message);
                return;
            }
            throw err;
        }
    }

    static async deleteQuery(msg) {
        logger.debug('Delete data of index with query ', msg.query);
        try {
            const elasticTaskId = await elasticService.deleteQuery(msg.index, msg.query);
            // Generate Performed Delete Query event
            await statusQueueService.sendPerformedDeleteQuery(msg.taskId, elasticTaskId);
        } catch (err) {
            if (err instanceof ElasticError) {
                await statusQueueService.sendErrorMessage(msg.taskId, err.message);
                return;
            }
            throw err;
        }

    }

    static async reindex(msg) {
        logger.debug(`Reindex from index ${msg.sourceIndex} to index ${msg.targetIndex}`);
        const elasticTaskId = await elasticService.reindex(msg.sourceIndex, msg.targetIndex);
        // Generate Performed Delete Query event
        await statusQueueService.sendPerformedReindex(msg.taskId, elasticTaskId);
    }

    static async confirmDelete(msg) {
        logger.debug('Confirm Delete data with elasticTaskId ', msg.elasticTaskId);
        const finished = await elasticService.checkFinishTaskId(msg.elasticTaskId);
        if (!finished) {
            throw new Error(`Delete index Elasticsearch task ${msg.elasticTaskId} not finished`);
        }
        // try {check elasticTask } catch (err) throw new Error
        // throwing an error here implies that the msg is going to
        // be "nacked"
        // set a timeout before throw the error
        // if not an error
        await statusQueueService.sendFinishedDeleteQuery(msg.taskId);
    }

    static async confirmReIndex(msg) {
        logger.debug('Confirm Reindex data with elasticTaskId ', msg.elasticTaskId);
        const finished = await elasticService.checkFinishTaskId(msg.elasticTaskId);
        if (!finished) {
            throw new Error(`Reindex Elasticsearch task ${msg.elasticTaskId} not finished`);
        }

        await statusQueueService.sendFinishedReindex(msg.taskId);
    }

    static async deleteIndex(msg) {
        logger.debug('Deleting index', msg.index);
        await elasticService.deleteIndex(msg.index);
        await statusQueueService.sendIndexDeleted(msg.taskId);
    }

    static async confirmImport(msg) {
        logger.debug('Confirming index', msg.index);
        await elasticService.activateIndex(msg.index);
        await statusQueueService.sendImportConfirmed(msg.taskId);
    }

}

module.exports = ExecutorService;

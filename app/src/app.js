const logger = require('logger');

// const nock = require('nock');
// nock.recorder.rec();

logger.debug('Initializing doc-executor');
require('services/executor-queue.service');
require('services/status-queue.service');

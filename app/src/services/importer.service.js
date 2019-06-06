const logger = require('logger');
const ConverterFactory = require('services/converters/converterFactory');
const _ = require('lodash');
const dataQueueService = require('services/data-queue.service');
const statusQueueService = require('services/status-queue.service');
const StamperyService = require('services/stamperyService');
const config = require('config');

const CONTAIN_SPACES = /\s/g;
const IS_NUMBER = /^\d+$/;

function isJSONObject(value) {
    if (isNaN(value) && /^[\],:{}\s]*$/.test(value.replace(/\\["\\\/bfnrtu]/g, '@').replace(/"[^"\\\n\r]*"|true|false|null|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?/g, ']').replace(/(?:^|:|,)(?:\s*\[)+/g, ''))) {
        return true;
    }
    return false;
}

function convertPointToGeoJSON(lat, long) {
    return {
        type: 'point',
        coordinates: [
            long,
            lat
        ]
    };
}

class ImporterService {

    constructor(msg) {
        this.body = [];
        this.datasetId = msg.datasetId;
        this.provider = msg.provider;
        this.url = msg.fileUrl;
        this.dataPath = msg.dataPath;
        this.verify = msg.verified;
        this.legend = msg.legend;
        this.taskId = msg.taskId;
        this.index = msg.index;
        this.type = msg.indexType;
        this.indexObj = {
            index: {
                _index: msg.index,
                _type: msg.indexType
            }
        };
        this.numPacks = 0;
    }

    async start() {
        return new Promise(async (resolve, reject) => {
            try {
                logger.debug('Starting read file');
                const converter = ConverterFactory.getInstance(this.provider, this.url, this.dataPath, this.verify);
                // StamperyService
                if (this.verify) {
                    const blockchain = await StamperyService.stamp(this.datasetId, converter.sha256, converter.filePath, this.type);
                    statusQueueService.sendBlockChainGenerated(this.taskId, blockchain);
                }
                await converter.init();
                const stream = converter.serialize();
                logger.debug('Starting process file');
                stream.on('data', this.processRow.bind(this, stream, reject));
                stream.on('error', (err) => {
                    logger.error('Error reading file', err);
                    reject(err);
                });
                stream.on('end', () => {
                    if (this.numPacks === 0 && this.body && this.body.length === 0) {
                        statusQueueService.sendErrorMessage(this.taskId, 'File empty');
                        resolve();
                        return;
                    }
                    logger.debug('Finishing reading file');
                    if (this.body && this.body.length > 0) {
                        // send last rows to data queue
                        dataQueueService.sendDataMessage(this.taskId, this.index, this.body).then(() => {
                            this.body = [];
                            logger.debug('Pack saved successfully, num:', ++this.numPacks);
                            resolve();
                        }, (err) => {
                            logger.error('Error saving ', err);
                            reject(err);
                        });
                    } else {
                        resolve();
                    }
                });
            } catch (err) {
                logger.error(err);
                reject(err);
            }
        });
    }

    async processRow(stream, reject, data) {
        try {
            stream.pause();
            try {
                if (_.isPlainObject(data)) {

                    _.forEach(data, (value, key) => {
                        let newKey = key;
                        try {
                            if (newKey !== '_id') {
                                if (CONTAIN_SPACES.test(key)) {
                                    delete data[key];
                                    newKey = key.replace(CONTAIN_SPACES, '_');
                                }
                                if (IS_NUMBER.test(newKey)) {
                                    if (data[newKey]) {
                                        delete data[key];
                                    }
                                    newKey = `col_${newKey}`;
                                }
                                if (newKey.indexOf('.') >= 0) {
                                    if (data[newKey]) {
                                        delete data[newKey];
                                    }
                                    newKey = newKey.replace(/\./g, '_');
                                }
                                if (!(value instanceof Object) && isJSONObject(value)) {
                                    try {
                                        data[newKey] = JSON.parse(value);
                                    } catch (e) {
                                        data[newKey] = value;
                                    }
                                } else if (!isNaN(value)) {
                                    data[newKey] = Number(value);
                                } else {
                                    data[newKey] = value;
                                }
                            } else {
                                delete data[newKey];
                            }
                        } catch (e) {
                            logger.error(e);
                            throw new Error(e);
                        }
                    });

                    if (this.legend && (this.legend.lat || this.legend.long)) {
                        if (data[this.legend.lat] && data[this.legend.long]) {
                            data.the_geom = convertPointToGeoJSON(data[this.legend.lat], data[this.legend.long]);
                            data.the_geom_point = {
                                lat: data[this.legend.lat],
                                lon: data[this.legend.long]
                            };
                        }
                    }
                    logger.trace('Adding new row');
                    this.body.push(this.indexObj);
                    this.body.push(data);

                } else {
                    logger.error('Data and/or options have no headers specified');
                }
            } catch (e) {
                // continue
                logger.error('Error generating', e);
            }

            if (this.body && this.body.length >= config.get('elementPerPackage')) {
                logger.debug('Sending data');

                dataQueueService.sendDataMessage(this.taskId, this.index, this.body).then(() => {
                    this.body = [];
                    stream.resume();
                    logger.debug('Pack saved successfully, num:', ++this.numPacks);
                }, (err) => {
                    logger.error('Error saving ', err);
                    stream.end();
                    reject(err);
                });
            } else {
                stream.resume();
            }
        } catch (err) {
            logger.error('Error saving', err);
        }
    }

}

module.exports = ImporterService;

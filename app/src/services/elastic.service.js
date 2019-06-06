const logger = require('logger');
const elasticsearch = require('elasticsearch');
const config = require('config');
const ElasticError = require('errors/elastic.error');

const elasticUrl = config.get('elastic.url');

class ElasticService {

    constructor() {
        const extendAPI = {
            explain: function (opts, cb) {
                const call = (err, data) => {
                    if (data) {
                        try {
                            cb(err, JSON.parse(data));
                            return;
                        } catch (e) {
                            cb(err, null);
                            return;
                        }
                    }
                    cb(err, data);
                    return;
                };
                logger.debug('Doing explain with', opts);
                this.transport.request({
                    method: 'POST',
                    path: encodeURI('/_sql/_explain'),
                    body: opts.sql
                }, call);
            }
        };
        elasticsearch.Client.apis.extendAPI = Object.assign({}, elasticsearch.Client.apis['5.6'], extendAPI);

        this.client = new elasticsearch.Client({
            host: elasticUrl,
            log: 'error',
            apiVersion: 'extendAPI'
        });
        // logger.debug('Doing ping to elastic');
        this.client.ping({
            // ping usually has a 3000ms timeout
            requestTimeout: 10000
        }, function (error) {
            if (error) {
                logger.error('elasticsearch cluster is down!');
                process.exit(1);
            }
        });
    }

    async createIndex(index, type, legend) {
        logger.debug(`Creating index ${index} and type ${type} in elastic`);
        if (!type) {
            type = index;
        }
        const body = {
            mappings: {
                [type]: {
                    properties: {}
                }
            }
        };
        if (legend && legend.lat && legend.long) {
            logger.debug('Adding geo column');
            body.mappings[type].properties.the_geom = {
                type: 'geo_shape',
                tree: 'geohash',
                precision: '1m',
                points_only: true
            };
            body.mappings[type].properties.the_geom_point = {
                type: 'geo_point'
            };
        }

        if (legend && legend.nested) {
            for (let i = 0, { length } = legend.nested; i < length; i++) {
                body.mappings[type].properties[legend.nested[i]] = {
                    type: 'nested',
                    include_in_parent: true
                };
            }
        }


        const fieldTypeList = [
            'integer',
            'short',
            'byte',
            'double',
            'float',
            'half_float',
            'scaled_float',
            'boolean',
            'binary',
            'text',
            'keyword'
        ];

        fieldTypeList.forEach((fieldType) => {
            if (legend && legend[fieldType]) {
                for (let i = 0, { length } = legend[fieldType]; i < length; i++) {
                    body.mappings[type].properties[legend[fieldType][i]] = {
                        type: fieldType
                    };
                }
            }
        })

        return new Promise((resolve, reject) => {

            this.client.indices.create({
                index,
                body
            }, (err) => {
                if (err) {
                    reject(new ElasticError(err));
                    return;
                }
                resolve(index);
            });
        });
    }

    async activateIndex(index) {
        return new Promise((resolve, reject) => {
            const options = {
                index,
                body: {
                    index: {
                        refresh_interval: '1s',
                        number_of_replicas: 1
                    }
                }
            };
            this.client.indices.putSettings(options, (error, response) => {
                if (error) {
                    reject(error);
                    return;
                }
                resolve(response);
            });
        });
    }

    async deactivateIndex(index) {
        return new Promise((resolve, reject) => {
            const options = {
                index,
                body: {
                    index: {
                        refresh_interval: '-1',
                        number_of_replicas: 0
                    }
                }
            };
            this.client.indices.putSettings(options, (error, response) => {
                if (error) {
                    reject(error);
                    return;
                }
                resolve(response);
            });
        });
    }

    async deleteIndex(index) {
        return new Promise((resolve, reject) => {
            this.client.indices.delete({
                index
            }, (error, response) => {
                if (error) {
                    reject(error);
                    return;
                }
                resolve(response);
            });
        });
    }

    async reindex(sourceIndex, destIndex) {
        return new Promise((resolve, reject) => {
            this.client.reindex({
                waitForCompletion: false,
                body: {
                    source: {
                        index: sourceIndex
                    },
                    dest: {
                        index: destIndex
                    }
                }
            }, (error, response) => {
                if (error) {
                    reject(error);
                    return;
                }
                resolve(response.task);
            });
        });
    }


    async deleteQuery(index, sql) {
        return new Promise((resolve, reject) => {
            logger.debug('Doing explain of query');
            this.client.explain({
                sql: sql.replace(/delete/gi, 'select * ')
            }, (err, resultQueryElastic) => {
                if (err) {
                    logger.error(err);
                    console.log(err);
                    if (err.statusCode === 500) {
                        reject(new ElasticError(err.message));
                    }
                    reject(err);
                    return;
                }
                delete resultQueryElastic.from;
                delete resultQueryElastic.size;
                logger.debug('Doing query');

                this.client.deleteByQuery({
                    index,
                    body: resultQueryElastic,
                    waitForCompletion: false
                }, (error, response) => {
                    if (error) {
                        reject(error);
                        return;
                    }
                    resolve(response.task);
                });
            });
        });
    }

    async checkFinishTaskId(taskId) {
        return new Promise((resolve, reject) => {
            this.client.tasks.get({
                taskId
            }, (err, data) => {
                if (err) {
                    reject(err);
                    return;
                }
                if (data && ((data.length > 0 && data[0].completed) || data.completed)) {
                    logger.debug('Task completed');
                    resolve(true);
                    return;
                }
                resolve(false);
            });
        });
    }

}

module.exports = new ElasticService();

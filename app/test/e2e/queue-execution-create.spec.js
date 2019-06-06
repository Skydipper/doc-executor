/* eslint-disable no-unused-vars,no-undef,no-await-in-loop */
const nock = require('nock');
const chai = require('chai');
const amqp = require('amqplib');
const config = require('config');
const RabbitMQConnectionError = require('errors/rabbitmq-connection.error');
const docImporterMessages = require('rw-doc-importer-messages');
const fs = require('fs');
const path = require('path');
const chaiMatch = require('chai-match');
const sleep = require('sleep');

const { getTestServer } = require('./test-server');

chai.use(chaiMatch);
const should = chai.should();

let requester;
let rabbitmqConnection = null;
let channel;

nock.disableNetConnect();
nock.enableNetConnect(process.env.HOST_IP);

describe('EXECUTION_CREATE handling process', () => {

    before(async () => {
        if (process.env.NODE_ENV !== 'test') {
            throw Error(`Running the test suite with NODE_ENV ${process.env.NODE_ENV} may result in permanent data loss. Please use NODE_ENV=test.`);
        }

        requester = await getTestServer();
    });

    beforeEach(async () => {
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

        channel = await rabbitmqConnection.createConfirmChannel();
        await channel.assertQueue(config.get('queues.executorTasks'));
        await channel.assertQueue(config.get('queues.status'));
        await channel.assertQueue(config.get('queues.data'));

        await channel.purgeQueue(config.get('queues.executorTasks'));
        await channel.purgeQueue(config.get('queues.status'));
        await channel.purgeQueue(config.get('queues.data'));

        const executorTasksQueueStatus = await channel.checkQueue(config.get('queues.executorTasks'));
        executorTasksQueueStatus.messageCount.should.equal(0);

        const statusQueueStatus = await channel.checkQueue(config.get('queues.status'));
        statusQueueStatus.messageCount.should.equal(0);

        const dataQueueStatus = await channel.checkQueue(config.get('queues.data'));
        dataQueueStatus.messageCount.should.equal(0);
    });

    it('Consume a EXECUTION_CREATE message and create a new task and STATUS_INDEX_CREATED, STATUS_READ_DATA and STATUS_READ_FILE messages (happy case)', async () => {
        const timestamp = new Date().getTime();

        nock(`http://${process.env.ELASTIC_URL}`)
            .put(new RegExp(`/index_${timestamp}_(\\w*)`), { mappings: { type: { properties: {} } } })
            .reply(200, { acknowledged: true, shards_acknowledged: true });


        nock(`http://${process.env.ELASTIC_URL}`)
            .put(new RegExp(`/index_${timestamp}_(\\w*)/_settings`), {
                index: {
                    refresh_interval: '-1',
                    number_of_replicas: 0
                }
            })
            .reply(200, { acknowledged: true });

        nock('http://api.resourcewatch.org')
            .get('/dataset')
            .reply(200, {
                data: JSON.parse(fs.readFileSync(path.join(__dirname, 'dataset-list.json'))),
                links: {
                    self: 'http://api.resourcewatch.org/v1/dataset?page[number]=1&page[size]=10',
                    first: 'http://api.resourcewatch.org/v1/dataset?page[number]=1&page[size]=10',
                    last: 'http://api.resourcewatch.org/v1/dataset?page[number]=150&page[size]=10',
                    prev: 'http://api.resourcewatch.org/v1/dataset?page[number]=1&page[size]=10',
                    next: 'http://api.resourcewatch.org/v1/dataset?page[number]=2&page[size]=10'
                },
                meta: { 'total-pages': 150, 'total-items': 1499, size: 10 }
            });

        const message = {
            id: 'a68931ad-d3f6-4447-9c0c-df415dd001cd',
            type: 'EXECUTION_CREATE',
            taskId: '1128cf58-4cd7-4eab-b2db-118584d945bf',
            datasetId: `${timestamp}`,
            fileUrl: 'http://api.resourcewatch.org/dataset',
            provider: 'json',
            legend: {},
            verified: false,
            dataPath: 'data',
            indexType: 'type',
            index: 'index_a9e4286f3b4e47ad8abbd2d1a084435b_1551683862824'
        };

        const preExecutorTasksQueueStatus = await channel.assertQueue(config.get('queues.executorTasks'));
        preExecutorTasksQueueStatus.messageCount.should.equal(0);
        const preStatusQueueStatus = await channel.assertQueue(config.get('queues.status'));
        preStatusQueueStatus.messageCount.should.equal(0);

        await channel.sendToQueue(config.get('queues.executorTasks'), Buffer.from(JSON.stringify(message)));

        // Give the code some time to do its thing
        await new Promise(resolve => setTimeout(resolve, 25000));

        const postExecutorTasksQueueStatus = await channel.assertQueue(config.get('queues.executorTasks'));
        postExecutorTasksQueueStatus.messageCount.should.equal(0);
        const postStatusQueueStatus = await channel.assertQueue(config.get('queues.status'));
        postStatusQueueStatus.messageCount.should.equal(3);
        const postDataQueueStatus = await channel.assertQueue(config.get('queues.data'));
        postDataQueueStatus.messageCount.should.equal(1);

        const validateDataQueueMessages = async (msg) => {
            const content = JSON.parse(msg.content.toString());
            try {
                switch (content.type) {

                    case docImporterMessages.data.MESSAGE_TYPES.DATA:
                        content.should.have.property('id');
                        content.should.have.property('index').and.match(new RegExp(`index_${timestamp}_(\\w*)`));
                        content.should.have.property('taskId').and.equal(message.taskId);
                        content.should.have.property('data');
                        break;
                    default:
                        throw new Error('Unexpected message type');

                }
            } catch (err) {
                throw err;
            }

            await channel.ack(msg);
        };

        const validateStatusQueueMessages = async (msg) => {
            const content = JSON.parse(msg.content.toString());
            try {
                switch (content.type) {

                    case docImporterMessages.status.MESSAGE_TYPES.STATUS_INDEX_CREATED:
                        content.should.have.property('id');
                        content.should.have.property('index').and.match(new RegExp(`index_${timestamp}_(\\w*)`));
                        content.should.have.property('taskId').and.equal(message.taskId);
                        break;
                    case docImporterMessages.status.MESSAGE_TYPES.STATUS_READ_DATA:
                        content.should.have.property('id');
                        content.should.have.property('taskId').and.equal(message.taskId);
                        break;
                    case docImporterMessages.status.MESSAGE_TYPES.STATUS_READ_FILE:
                        content.should.have.property('id');
                        content.should.have.property('taskId').and.equal(message.taskId);
                        break;
                    default:
                        throw new Error('Unexpected message type');

                }
            } catch (err) {
                throw err;
            }

            await channel.ack(msg);
        };

        await channel.consume(config.get('queues.status'), validateStatusQueueMessages);
        await channel.consume(config.get('queues.data'), validateDataQueueMessages);

        process.on('unhandledRejection', (error) => {
            should.fail(error);
        });
    });

    it('Consume a EXECUTION_CREATE message with custom mappings and create a new task and STATUS_INDEX_CREATED, STATUS_READ_DATA and STATUS_READ_FILE messages (happy case)', async () => {
        const timestamp = new Date().getTime();

        nock(`http://${process.env.ELASTIC_URL}`)
            .put(new RegExp(`/index_${timestamp}_(\\w*)`), {
                mappings: {
                    type: {
                        properties: {
                            adm1: {
                                type: 'integer'
                            },
                            adm2: {
                                type: 'integer'
                            },
                            threshold_2000: {
                                type: 'integer'
                            },
                            ifl: {
                                type: 'integer'
                            },
                            'year_data.year': {
                                type: 'integer'
                            },
                            total_area: {
                                type: 'double'
                            },
                            total_gain: {
                                type: 'double'
                            },
                            total_biomass: {
                                type: 'double'
                            },
                            total_co2: {
                                type: 'double'
                            },
                            mean_biomass_per_ha: {
                                type: 'double'
                            },
                            total_mangrove_biomass: {
                                type: 'double'
                            },
                            total_mangrove_co2: {
                                type: 'double'
                            },
                            mean_mangrove_biomass_per_ha: {
                                type: 'double'
                            },
                            'year_data.area_loss': {
                                type: 'double'
                            },
                            'year_data.biomass_loss': {
                                type: 'double'
                            },
                            'year_data.carbon_emissions': {
                                type: 'double'
                            },
                            'year_data.mangrove_biomass_loss': {
                                type: 'double'
                            },
                            'year_data.mangrove_carbon_emissions': {
                                type: 'double'
                            },
                            primary_forest: {
                                type: 'boolean'
                            },
                            idn_primary_forest: {
                                type: 'boolean'
                            },
                            biodiversity_significance: {
                                type: 'boolean'
                            },
                            biodiversity_intactness: {
                                type: 'boolean'
                            },
                            'aze.year': {
                                type: 'boolean'
                            },
                            urban_watershed: {
                                type: 'boolean'
                            },
                            mangroves_1996: {
                                type: 'boolean'
                            },
                            mangroves_2016: {
                                type: 'boolean'
                            },
                            endemic_bird_area: {
                                type: 'boolean'
                            },
                            tiger_cl: {
                                type: 'boolean'
                            },
                            landmark: {
                                type: 'boolean'
                            },
                            land_right: {
                                type: 'boolean'
                            },
                            kba: {
                                type: 'boolean'
                            },
                            mining: {
                                type: 'boolean'
                            },
                            idn_mys_peatlands: {
                                type: 'boolean'
                            },
                            oil_palm: {
                                type: 'boolean'
                            },
                            idn_forest_moratorium: {
                                type: 'boolean'
                            },
                            mex_protected_areas: {
                                type: 'boolean'
                            },
                            mex_pes: {
                                type: 'boolean'
                            },
                            per_production_forest: {
                                type: 'boolean'
                            },
                            per_protected_area: {
                                type: 'boolean'
                            },
                            wood_fiber: {
                                type: 'boolean'
                            },
                            resource_right: {
                                type: 'boolean'
                            },
                            managed_forests: {
                                type: 'boolean'
                            },
                            oil_gas: {
                                type: 'boolean'
                            },
                            iso: {
                                type: 'text'
                            },
                            global_land_cover: {
                                type: 'text'
                            },
                            tsc: {
                                type: 'text'
                            },
                            erosion: {
                                type: 'text'
                            },
                            wdpa: {
                                type: 'text'
                            },
                            plantations: {
                                type: 'text'
                            },
                            river_basin: {
                                type: 'text'
                            },
                            ecozone: {
                                type: 'text'
                            },
                            water_stress: {
                                type: 'text'
                            },
                            rspo: {
                                type: 'text'
                            },
                            idn_land_cover: {
                                type: 'text'
                            },
                            mex_forest_zoning: {
                                type: 'text'
                            },
                            per_forest_concession: {
                                type: 'text'
                            },
                            bra_biomes: {
                                type: 'text'
                            }
                        }
                    }
                }
            })
            .reply(200, { acknowledged: true, shards_acknowledged: true });


        nock(`http://${process.env.ELASTIC_URL}`)
            .put(new RegExp(`/index_${timestamp}_(\\w*)/_settings`), {
                index: {
                    refresh_interval: '-1',
                    number_of_replicas: 0
                }
            })
            .reply(200, { acknowledged: true });

        nock('http://api.resourcewatch.org')
            .get('/dataset')
            .reply(200, {
                data: JSON.parse(fs.readFileSync(path.join(__dirname, 'dataset-list.json'))),
                links: {
                    self: 'http://api.resourcewatch.org/v1/dataset?page[number]=1&page[size]=10',
                    first: 'http://api.resourcewatch.org/v1/dataset?page[number]=1&page[size]=10',
                    last: 'http://api.resourcewatch.org/v1/dataset?page[number]=150&page[size]=10',
                    prev: 'http://api.resourcewatch.org/v1/dataset?page[number]=1&page[size]=10',
                    next: 'http://api.resourcewatch.org/v1/dataset?page[number]=2&page[size]=10'
                },
                meta: { 'total-pages': 150, 'total-items': 1499, size: 10 }
            });

        const message = {
            id: 'a68931ad-d3f6-4447-9c0c-df415dd001cd',
            type: 'EXECUTION_CREATE',
            taskId: '1128cf58-4cd7-4eab-b2db-118584d945bf',
            datasetId: `${timestamp}`,
            fileUrl: 'http://api.resourcewatch.org/dataset',
            provider: 'json',
            legend: {
                text: [
                    'iso', 'global_land_cover', 'tsc', 'erosion', 'wdpa', 'plantations', 'river_basin', 'ecozone', 'water_stress', 'rspo', 'idn_land_cover', 'mex_forest_zoning', 'per_forest_concession', 'bra_biomes'],
                integer: ['adm1', 'adm2', 'threshold_2000', 'ifl', 'year_data.year'],
                boolean: ['primary_forest', 'idn_primary_forest', 'biodiversity_significance', 'biodiversity_intactness', 'aze.year', 'urban_watershed', 'mangroves_1996', 'mangroves_2016', 'endemic_bird_area', 'tiger_cl', 'landmark', 'land_right', 'kba', 'mining', 'idn_mys_peatlands', 'oil_palm', 'idn_forest_moratorium', 'mex_protected_areas', 'mex_pes', 'per_production_forest', 'per_protected_area', 'wood_fiber', 'resource_right', 'managed_forests', 'oil_gas'],
                double: ['total_area', 'total_gain', 'total_biomass', 'total_co2', 'mean_biomass_per_ha', 'total_mangrove_biomass', 'total_mangrove_co2', 'mean_mangrove_biomass_per_ha', 'year_data.area_loss', 'year_data.biomass_loss', 'year_data.carbon_emissions', 'year_data.mangrove_biomass_loss', 'year_data.mangrove_carbon_emissions']
            },
            verified: false,
            dataPath: 'data',
            indexType: 'type',
            index: 'index_a9e4286f3b4e47ad8abbd2d1a084435b_1551683862824'
        };

        const preExecutorTasksQueueStatus = await channel.assertQueue(config.get('queues.executorTasks'));
        preExecutorTasksQueueStatus.messageCount.should.equal(0);
        const preStatusQueueStatus = await channel.assertQueue(config.get('queues.status'));
        preStatusQueueStatus.messageCount.should.equal(0);

        await channel.sendToQueue(config.get('queues.executorTasks'), Buffer.from(JSON.stringify(message)));

        // Give the code some time to do its thing
        await new Promise(resolve => setTimeout(resolve, 25000));

        const postExecutorTasksQueueStatus = await channel.assertQueue(config.get('queues.executorTasks'));
        postExecutorTasksQueueStatus.messageCount.should.equal(0);
        const postStatusQueueStatus = await channel.assertQueue(config.get('queues.status'));
        postStatusQueueStatus.messageCount.should.equal(3);
        const postDataQueueStatus = await channel.assertQueue(config.get('queues.data'));
        postDataQueueStatus.messageCount.should.equal(1);

        const validateDataQueueMessages = async (msg) => {
            const content = JSON.parse(msg.content.toString());
            try {
                switch (content.type) {

                    case docImporterMessages.data.MESSAGE_TYPES.DATA:
                        content.should.have.property('id');
                        content.should.have.property('index').and.match(new RegExp(`index_${timestamp}_(\\w*)`));
                        content.should.have.property('taskId').and.equal(message.taskId);
                        content.should.have.property('data');
                        break;
                    default:
                        throw new Error('Unexpected message type');

                }
            } catch (err) {
                throw err;
            }

            await channel.ack(msg);
        };

        const validateStatusQueueMessages = async (msg) => {
            const content = JSON.parse(msg.content.toString());
            try {
                switch (content.type) {

                    case docImporterMessages.status.MESSAGE_TYPES.STATUS_INDEX_CREATED:
                        content.should.have.property('id');
                        content.should.have.property('index').and.match(new RegExp(`index_${timestamp}_(\\w*)`));
                        content.should.have.property('taskId').and.equal(message.taskId);
                        break;
                    case docImporterMessages.status.MESSAGE_TYPES.STATUS_READ_DATA:
                        content.should.have.property('id');
                        content.should.have.property('taskId').and.equal(message.taskId);
                        break;
                    case docImporterMessages.status.MESSAGE_TYPES.STATUS_READ_FILE:
                        content.should.have.property('id');
                        content.should.have.property('taskId').and.equal(message.taskId);
                        break;
                    default:
                        throw new Error('Unexpected message type');

                }
            } catch (err) {
                throw err;
            }

            await channel.ack(msg);
        };

        await channel.consume(config.get('queues.status'), validateStatusQueueMessages);
        await channel.consume(config.get('queues.data'), validateDataQueueMessages);

        process.on('unhandledRejection', (error) => {
            should.fail(error);
        });
    });

    afterEach(async () => {
        await channel.assertQueue(config.get('queues.executorTasks'));
        await channel.purgeQueue(config.get('queues.executorTasks'));
        const executorQueueStatus = await channel.checkQueue(config.get('queues.executorTasks'));
        executorQueueStatus.messageCount.should.equal(0);

        await channel.assertQueue(config.get('queues.status'));
        await channel.purgeQueue(config.get('queues.status'));
        const statusQueueStatus = await channel.checkQueue(config.get('queues.status'));
        statusQueueStatus.messageCount.should.equal(0);

        await channel.assertQueue(config.get('queues.data'));
        await channel.purgeQueue(config.get('queues.data'));
        const dataQueueStatus = await channel.checkQueue(config.get('queues.data'));
        dataQueueStatus.messageCount.should.equal(0);

        if (!nock.isDone()) {
            throw new Error(`Not all nock interceptors were used: ${nock.pendingMocks()}`);
        }

        await rabbitmqConnection.close();
        rabbitmqConnection = null;
    });

    after(async () => {
    });
});

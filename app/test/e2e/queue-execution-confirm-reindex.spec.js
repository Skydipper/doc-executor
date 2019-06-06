/* eslint-disable no-unused-vars,no-undef,no-await-in-loop */
const nock = require('nock');
const chai = require('chai');
const amqp = require('amqplib');
const config = require('config');
const RabbitMQConnectionError = require('errors/rabbitmq-connection.error');
const docImporterMessages = require('rw-doc-importer-messages');
const sleep = require('sleep');

const { getTestServer } = require('./test-server');

const should = chai.should();

let requester;
let rabbitmqConnection = null;
let channel;

nock.disableNetConnect();
nock.enableNetConnect(process.env.HOST_IP);

describe('EXECUTION_CONFIRM_REINDEX handling process', () => {

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

    it('Consume a EXECUTION_CONFIRM_REINDEX message should check that the ES reindex task is done and send STATUS_FINISHED_REINDEX message if task is completed (happy case)', async () => {

        const message = {
            id: 'e27d387a-dd78-43b4-aa06-37f2fd44ce81',
            type: 'EXECUTION_CONFIRM_REINDEX',
            taskId: '178eac45-19b1-46d5-ac75-1005795b2993',
            elasticTaskId: '123456'
        };

        nock(`http://${process.env.ELASTIC_URL}`)
            .get(`/_tasks/${message.elasticTaskId}`)
            .reply(200, {
                completed: true,
                task: {
                    node: 'n8Jmh18uQOKvRYbm2XJiOg',
                    id: 707662,
                    type: 'transport',
                    action: 'indices:data/write/reindex',
                    status: {
                        total: 77727683,
                        updated: 0,
                        created: 77727683,
                        deleted: 0,
                        batches: 77728,
                        version_conflicts: 0,
                        noops: 0,
                        retries: {
                            bulk: 0,
                            search: 0
                        },
                        throttled_millis: 0,
                        requests_per_second: -1.0,
                        throttled_until_millis: 0
                    },
                    description: 'reindex from [index_db34c2d977b843eeb101f499e39d1597_1513782529481] to [index_db34c2d977b843eeb101f499e39d1597]',
                    start_time_in_millis: 1513800578282,
                    running_time_in_nanos: 12642725646966,
                    cancellable: true
                },
                response: {
                    took: 12642625,
                    timed_out: false,
                    total: 77727683,
                    updated: 0,
                    created: 77727683,
                    deleted: 0,
                    batches: 77728,
                    version_conflicts: 0,
                    noops: 0,
                    retries: {
                        bulk: 0,
                        search: 0
                    },
                    throttled_millis: 0,
                    requests_per_second: -1.0,
                    throttled_until_millis: 0,
                    failures: []
                }
            });


        const preExecutorTasksQueueStatus = await channel.assertQueue(config.get('queues.executorTasks'));
        preExecutorTasksQueueStatus.messageCount.should.equal(0);
        const preStatusQueueStatus = await channel.assertQueue(config.get('queues.status'));
        preStatusQueueStatus.messageCount.should.equal(0);

        await channel.sendToQueue(config.get('queues.executorTasks'), Buffer.from(JSON.stringify(message)));

        // Give the code 5 seconds to do its thing
        await new Promise(resolve => setTimeout(resolve, 5000));

        const postExecutorTasksQueueStatus = await channel.assertQueue(config.get('queues.executorTasks'));
        postExecutorTasksQueueStatus.messageCount.should.equal(0);
        const postStatusQueueStatus = await channel.assertQueue(config.get('queues.status'));
        postStatusQueueStatus.messageCount.should.equal(1);
        const postDataQueueStatus = await channel.assertQueue(config.get('queues.data'));
        postDataQueueStatus.messageCount.should.equal(0);

        const validateStatusQueueMessages = async (msg) => {
            const content = JSON.parse(msg.content.toString());

            content.should.have.property('type').and.equal(docImporterMessages.status.MESSAGE_TYPES.STATUS_FINISHED_REINDEX);
            content.should.have.property('id');
            content.should.have.property('taskId').and.equal(message.taskId);

            await channel.ack(msg);
        };

        await channel.consume(config.get('queues.status'), validateStatusQueueMessages);

        process.on('unhandledRejection', (error) => {
            should.fail(error);
        });
    });


    it('Consume a EXECUTION_CONFIRM_REINDEX message should check that the ES reindex task is done and send STATUS_ERROR message after 10 failed retries', async () => {

        const message = {
            id: 'e27d387a-dd78-43b4-aa06-37f2fd44ce81',
            type: 'EXECUTION_CONFIRM_REINDEX',
            taskId: '178eac45-19b1-46d5-ac75-1005795b2993',
            elasticTaskId: '234567'
        };

        nock(`http://${process.env.ELASTIC_URL}`)
            .get(`/_tasks/${message.elasticTaskId}`)
            .times(parseInt(config.get('messageRetries'), 10) + 1)
            .reply(200, {
                completed: false
            });


        const preExecutorTasksQueueStatus = await channel.assertQueue(config.get('queues.executorTasks'));
        preExecutorTasksQueueStatus.messageCount.should.equal(0);
        const preStatusQueueStatus = await channel.assertQueue(config.get('queues.status'));
        preStatusQueueStatus.messageCount.should.equal(0);

        await channel.sendToQueue(config.get('queues.executorTasks'), Buffer.from(JSON.stringify(message)));

        // Give the code 30 seconds to do its thing
        await new Promise(resolve => setTimeout(resolve, 5000));

        const postExecutorTasksQueueStatus = await channel.assertQueue(config.get('queues.executorTasks'));
        postExecutorTasksQueueStatus.messageCount.should.equal(0);
        const postStatusQueueStatus = await channel.assertQueue(config.get('queues.status'));
        postStatusQueueStatus.messageCount.should.equal(1);
        const postDataQueueStatus = await channel.assertQueue(config.get('queues.data'));
        postDataQueueStatus.messageCount.should.equal(0);

        const validateStatusQueueMessages = async (msg) => {
            const content = JSON.parse(msg.content.toString());

            content.should.have.property('type').and.equal(docImporterMessages.status.MESSAGE_TYPES.STATUS_ERROR);
            content.should.have.property('id');
            content.should.have.property('taskId').and.equal(message.taskId);
            content.should.have.property('error').and.equal('Exceeded maximum number of attempts to process the message');

            await channel.ack(msg);
        };

        await channel.consume(config.get('queues.status'), validateStatusQueueMessages);

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
});

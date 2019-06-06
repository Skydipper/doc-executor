/* eslint-disable no-unused-vars,no-undef,no-await-in-loop */
const nock = require('nock');
const chai = require('chai');
const amqp = require('amqplib');
const config = require('config');
const RabbitMQConnectionError = require('errors/rabbitmq-connection.error');
const docImporterMessages = require('rw-doc-importer-messages');
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

describe('EXECUTION_DELETE_INDEX handling process', () => {

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

    it('Consume a EXECUTION_DELETE_INDEX message and create a new task and STATUS_INDEX_CREATED, STATUS_READ_DATA and STATUS_READ_FILE messages (happy case)', async () => {
        const message = {
            id: '3051e9b8-30dc-424d-a4f7-d4f77bf08688',
            type: 'EXECUTION_DELETE_INDEX',
            taskId: '1392139b-5e64-4301-9389-24d43ca6279b',
            index: '4f00e8fb-6f28-42e9-9549-fb7d72e67ed7'
        };

        nock(`http://${process.env.ELASTIC_URL}`)
            .delete(`/${message.index}`)
            .reply(200, {});

        const preExecutorTasksQueueStatus = await channel.assertQueue(config.get('queues.executorTasks'));
        preExecutorTasksQueueStatus.messageCount.should.equal(0);
        const preStatusQueueStatus = await channel.assertQueue(config.get('queues.status'));
        preStatusQueueStatus.messageCount.should.equal(0);

        await channel.sendToQueue(config.get('queues.executorTasks'), Buffer.from(JSON.stringify(message)));

        // Give the code 3 seconds to do its thing
        await new Promise(resolve => setTimeout(resolve, 15000));

        const postExecutorTasksQueueStatus = await channel.assertQueue(config.get('queues.executorTasks'));
        postExecutorTasksQueueStatus.messageCount.should.equal(0);
        const postStatusQueueStatus = await channel.assertQueue(config.get('queues.status'));
        postStatusQueueStatus.messageCount.should.equal(1);
        const postDataQueueStatus = await channel.assertQueue(config.get('queues.data'));
        postDataQueueStatus.messageCount.should.equal(0);


        const validateStatusQueueMessages = async (msg) => {
            const content = JSON.parse(msg.content.toString());
            try {
                switch (content.type) {

                    case docImporterMessages.status.MESSAGE_TYPES.STATUS_INDEX_DELETED:
                        content.should.have.property('id');
                        content.should.have.property('taskId').and.equal(message.taskId);
                        content.should.have.property('lastCheckedDate');
                        break;
                    default:
                        throw new Error(`Unexpected message type: ${content.type}`);

                }
            } catch (err) {
                throw err;
            }

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

    after(async () => {
    });
});

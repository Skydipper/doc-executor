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

describe('EXECUTION_CONFIRM_IMPORT handling process', () => {

    before(async () => {
        if (process.env.NODE_ENV !== 'test') {
            throw Error(`Running the test suite with NODE_ENV ${process.env.NODE_ENV} may result in permanent data loss. Please use NODE_ENV=test.`);
        }

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

        requester = await getTestServer();
    });

    beforeEach(async () => {
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

    it('Consume a EXECUTION_CONFIRM_IMPORT message should activate ES index and send STATUS_IMPORT_CONFIRMED message (happy case)', async () => {
        nock(`http://${process.env.ELASTIC_URL}`)
            .put('/index_6d0fa8f3164d46fa86628a5179f23fbc_1553845448826/_settings',
                { index: { refresh_interval: '1s', number_of_replicas: 1 } })
            .reply(200, { acknowledged: true });


        const message = {
            id: 'e27d387a-dd78-43b4-aa06-37f2fd44ce81',
            type: 'EXECUTION_CONFIRM_IMPORT',
            taskId: '178eac45-19b1-46d5-ac75-1005795b2993',
            index: 'index_6d0fa8f3164d46fa86628a5179f23fbc_1553845448826'
        };

        const preExecutorTasksQueueStatus = await channel.assertQueue(config.get('queues.executorTasks'));
        preExecutorTasksQueueStatus.messageCount.should.equal(0);
        const preStatusQueueStatus = await channel.assertQueue(config.get('queues.status'));
        preStatusQueueStatus.messageCount.should.equal(0);

        await channel.sendToQueue(config.get('queues.executorTasks'), Buffer.from(JSON.stringify(message)));

        // Give the code 3 seconds to do its thing
        await new Promise(resolve => setTimeout(resolve, 3000));

        const postExecutorTasksQueueStatus = await channel.assertQueue(config.get('queues.executorTasks'));
        postExecutorTasksQueueStatus.messageCount.should.equal(0);
        const postStatusQueueStatus = await channel.assertQueue(config.get('queues.status'));
        postStatusQueueStatus.messageCount.should.equal(1);
        const postDataQueueStatus = await channel.assertQueue(config.get('queues.data'));
        postDataQueueStatus.messageCount.should.equal(0);

        const validateStatusQueueMessages = async (msg) => {
            const content = JSON.parse(msg.content.toString());

            content.should.have.property('type').and.equal(docImporterMessages.status.MESSAGE_TYPES.STATUS_IMPORT_CONFIRMED);
            content.should.have.property('id');
            content.should.have.property('taskId').and.equal(message.taskId);

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

        if (!nock.isDone()) {
            throw new Error(`Not all nock interceptors were used: ${nock.pendingMocks()}`);
        }
    });

    after(async () => {
        await channel.purgeQueue(config.get('queues.executorTasks'));
        await channel.purgeQueue(config.get('queues.status'));
        await channel.purgeQueue(config.get('queues.data'));

        rabbitmqConnection.close();
    });
});

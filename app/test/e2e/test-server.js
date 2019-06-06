const nock = require('nock');
const chai = require('chai');
const chaiHttp = require('chai-http');

let requester;

chai.use(chaiHttp);

exports.getTestServer = function getTestServer() {
    if (requester) {
        return requester;
    }

    nock(`http://${process.env.ELASTIC_URL}`)
        .head('/')
        .reply(200);

    const server = require('../../src/app');
    requester = chai.request(server).keepOpen();

    return requester;
};

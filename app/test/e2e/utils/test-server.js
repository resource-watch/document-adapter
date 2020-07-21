const nock = require('nock');
const chai = require('chai');
const config = require('config');
const chaiHttp = require('chai-http');

let requester;

chai.use(chaiHttp);

exports.getTestServer = function getTestServer() {
    if (requester) {
        return requester;
    }

    nock(process.env.CT_URL)
        .post(`/api/v1/microservice`)
        .reply(200);

    const elasticUri = process.env.ELASTIC_URI || `${config.get('elasticsearch.host')}:${config.get('elasticsearch.port')}`;

    nock(elasticUri)
        .head('/')
        .times(999999)
        .reply(200);

    const server = require('../../../src/app');
    requester = chai.request(server).keepOpen();

    return requester;
};

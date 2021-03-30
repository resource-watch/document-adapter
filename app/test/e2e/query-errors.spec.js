const nock = require('nock');
const chai = require('chai');
const uuid = require('uuid');
const { getTestServer } = require('./utils/test-server');
const {
    createMockGetDataset, createIndex, insertData, deleteTestIndeces
} = require('./utils/helpers');

chai.should();

const requester = getTestServer();

nock.disableNetConnect();
nock.enableNetConnect((host) => [`${process.env.HOST_IP}:${process.env.PORT}`, process.env.ELASTIC_TEST_URL].includes(host));

describe('Query datasets - Errors', () => {

    before(async () => {
        if (process.env.NODE_ENV !== 'test') {
            throw Error(`Running the test suite with NODE_ENV ${process.env.NODE_ENV} may result in permanent data loss. Please use NODE_ENV=test.`);
        }
    });

    it('Invalid query to dataset should return meaningful error message and error code', async () => {
        const datasetId = uuid.v4();

        createMockGetDataset(datasetId);

        const requestBody = {
            loggedUser: null
        };

        const query = `potato ${datasetId}`;

        nock(process.env.CT_URL)
            .get('/v1/convert/sql2SQL')
            .query({ sql: `potato ${datasetId}` })
            .reply(400, { errors: [{ status: 400, detail: 'Malformed query' }] }, ['Vary',
                'Origin',
                'Content-Type',
                'application/vnd.api+json',
                'X-Response-Time',
                '13 ms',
                'Content-Length',
                '54',
                'Date',
                'Wed, 19 Sep 2018 07:34:38 GMT',
                'Connection',
                'close']);

        const queryResponse = await requester
            .post(`/api/v1/document/query/csv/${datasetId}?sql=${query}`)
            .send(requestBody);

        queryResponse.status.should.equal(400);
        queryResponse.body.should.have.property('errors').and.be.an('array').and.have.lengthOf(1);
        queryResponse.body.errors[0].detail.should.include('Malformed query');
    });

    it('Query with invalid function call should return meaningful error message and error code', async () => {
        const datasetId = uuid.v4();

        createMockGetDataset(datasetId);

        const requestBody = {
            loggedUser: null
        };

        const query = `queryResponse.body.errors[0] ${datasetId}`;

        nock(process.env.CT_URL)
            .get('/v1/convert/sql2SQL')
            .query({ sql: query })
            .reply(400, {
                errors: [
                    {
                        status: 400,
                        detail: 'Unsupported query element detected: queryResponse.body.errors[0]'
                    }
                ]
            });

        const queryResponse = await requester
            .post(`/api/v1/document/query/csv/${datasetId}?sql=${query}`)
            .send(requestBody);

        queryResponse.status.should.equal(400);
        queryResponse.body.should.have.property('errors').and.be.an('array').and.have.lengthOf(1);
        queryResponse.body.errors[0].detail.should.include('Unsupported query element detected');
    });

    it('Query with aggregation for invalid type should return meaningful error message and error code', async () => {
        const datasetId = uuid.v4();

        const dataset = createMockGetDataset(datasetId);
        const fieldsStructure = {
            year_date: { type: 'date' }
        };

        const results = [
            {
                year_date: '2015-01-01T00:00:00'
            }, {
                year_date: '2015-06-06T00:00:00'
            }, {
                year_date: '2015-03-03T00:00:00'
            }, {
                year_date: '2015-02-02T00:00:00'
            }
        ];

        await createIndex(dataset.attributes.tableName, fieldsStructure);
        await insertData(dataset.attributes.tableName, results);

        const query = `SELECT MIN(year_date) AS min, MAX(year_date) AS max FROM ${datasetId}`;

        nock(process.env.CT_URL)
            .get('/v1/convert/sql2SQL')
            .query({ sql: query })
            .reply(200, {
                data: {
                    type: 'result',
                    attributes: {
                        query: `SELECT MIN(year_date) AS min, MAX(year_date) AS max FROM ${datasetId}`,
                        jsonSql: {
                            select: [
                                {
                                    type: 'function',
                                    alias: 'min',
                                    value: 'MIN',
                                    arguments: [
                                        {
                                            value: 'year_date',
                                            type: 'literal'
                                        }
                                    ]
                                },
                                {
                                    type: 'function',
                                    alias: 'max',
                                    value: 'MAX',
                                    arguments: [
                                        {
                                            value: 'year_date',
                                            type: 'literal'
                                        }
                                    ]
                                }
                            ],
                            from: datasetId
                        }
                    }
                }
            });

        const queryResponse = await requester
            .post(`/api/v1/document/query/csv/${datasetId}?sql=${query}`)
            .send();

        queryResponse.status.should.equal(400);
        queryResponse.body.should.have.property('errors').and.be.an('array').and.have.lengthOf(1);
        queryResponse.body.errors[0].detail.should.include('Function [MIN] cannot work with [DATE]. Usage: MIN(NUMBER T) -> T');
    });

    afterEach(async () => {
        await deleteTestIndeces();

        if (!nock.isDone()) {
            const pendingMocks = nock.pendingMocks();
            throw new Error(`Not all nock interceptors were used: ${pendingMocks}`);
        }

    });
});

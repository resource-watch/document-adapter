/* eslint-disable max-len */
const nock = require('nock');
const chai = require('chai');
const uuid = require('uuid');
const { getTestServer } = require('./utils/test-server');
const {
    createMockGetDataset, createIndex, deleteTestIndeces, insertData, hasOpenScrolls
} = require('./utils/helpers');

chai.should();

const requester = getTestServer();

nock.disableNetConnect();
nock.enableNetConnect((host) => [`${process.env.HOST_IP}:${process.env.PORT}`, process.env.ELASTIC_TEST_URL].includes(host));

describe('Query datasets - Simple queries - POST HTTP verb', () => {

    before(async () => {
        if (process.env.NODE_ENV !== 'test') {
            throw Error(`Running the test suite with NODE_ENV ${process.env.NODE_ENV} may result in permanent data loss. Please use NODE_ENV=test.`);
        }
    });

    beforeEach(async () => {
        await deleteTestIndeces();
    });

    it('Query to dataset without connectorType document should fail', async () => {
        const datasetId = uuid.v4();

        createMockGetDataset(datasetId, { connectorType: 'foo' });

        const requestBody = {
            loggedUser: null
        };

        const query = `select * from ${datasetId}`;

        const queryResponse = await requester
            .post(`/api/v1/document/query/csv/${datasetId}?sql=${encodeURI(query)}`)
            .send(requestBody);

        queryResponse.status.should.equal(422);
        queryResponse.body.should.have.property('errors').and.be.an('array').and.have.lengthOf(1);
        queryResponse.body.errors[0].detail.should.include('This operation is only supported for datasets with connectorType \'document\'');
    });

    it('Query to dataset without a supported provider should fail', async () => {
        const datasetId = uuid.v4();

        createMockGetDataset(datasetId, { provider: 'foo' });

        const requestBody = {
            loggedUser: null
        };

        const query = `select * from ${datasetId}`;

        const queryResponse = await requester
            .post(`/api/v1/document/query/csv/${datasetId}?sql=${encodeURI(query)}`)
            .send(requestBody);

        queryResponse.status.should.equal(422);
        queryResponse.body.should.have.property('errors').and.be.an('array').and.have.lengthOf(1);
        queryResponse.body.errors[0].detail.should.include('This operation is only supported for datasets with provider [\'json\', \'csv\', \'tsv\', \'xml\']');
    });

    it('Query an empty dataset should be successful (happy case)', async () => {
        const datasetId = uuid.v4();

        createMockGetDataset(datasetId);

        const requestBody = {
            loggedUser: null
        };

        const query = `select * from ${datasetId}`;

        nock(process.env.CT_URL)
            .get(`/v1/convert/sql2SQL`)
            .query({
                sql: query
            })
            .once()
            .reply(200, {
                status: 200,
                data: {
                    type: 'result',
                    id: 'undefined',
                    attributes: {
                        query: `SELECT * FROM ${datasetId}`,
                        jsonSql: {
                            select: [
                                {
                                    value: '*',
                                    alias: null,
                                    type: 'wildcard'
                                }
                            ],
                            from: datasetId
                        }
                    },
                    relationships: {}
                }
            });

        await createIndex(
            'test_index_d1ced4227cd5480a8904d3410d75bf42_1587619728489',
            {
                adm1: { type: 'long' },
                adm2: { type: 'long' },
                area: { type: 'float' },
                iso: {
                    type: 'text',
                    fields: { keyword: { type: 'keyword', ignore_above: 256 } }
                },
                thresh: { type: 'long' }
            }
        );

        const queryResponse = await requester
            .post(`/api/v1/document/query/csv/${datasetId}?sql=${encodeURI(query)}`)
            .send(requestBody);

        queryResponse.status.should.equal(200);
        queryResponse.body.should.have.property('data').and.be.an('array');
        queryResponse.body.should.have.property('meta').and.be.an('object');

        queryResponse.body.data.should.have.lengthOf(0);
        (await hasOpenScrolls()).should.equal(false);
    });

    it('Basic query to dataset should be successful (happy case)', async () => {
        const datasetId = uuid.v4();

        createMockGetDataset(datasetId);

        const requestBody = {
            loggedUser: null
        };

        const query = `select * from ${datasetId}`;

        const results = [
            {
                thresh: 75, iso: 'USA', adm1: 27, adm2: 1641, area: 41420.47960515353
            }, {
                thresh: 75, iso: 'BRA', adm1: 12, adm2: 1450, area: 315602.3928570104
            }, {
                thresh: 75, iso: 'RUS', adm1: 35, adm2: 925, area: 1137359.9711284428
            }, {
                thresh: 75, iso: 'COL', adm1: 30, adm2: 1017, area: 128570.48945388374
            }
        ];

        nock(process.env.CT_URL)
            .get(`/v1/convert/sql2SQL`)
            .query({
                sql: query
            })
            .once()
            .reply(200, {
                status: 200,
                data: {
                    type: 'result',
                    id: 'undefined',
                    attributes: {
                        query: `SELECT * FROM ${datasetId}`,
                        jsonSql: {
                            select: [
                                {
                                    value: '*',
                                    alias: null,
                                    type: 'wildcard'
                                }
                            ],
                            from: datasetId
                        }
                    },
                    relationships: {}
                }
            });

        await createIndex(
            'test_index_d1ced4227cd5480a8904d3410d75bf42_1587619728489',
            {
                adm1: { type: 'long' },
                adm2: { type: 'long' },
                area: { type: 'float' },
                iso: {
                    type: 'text',
                    fields: { keyword: { type: 'keyword', ignore_above: 256 } }
                },
                thresh: { type: 'long' }
            }
        );

        await insertData(
            'test_index_d1ced4227cd5480a8904d3410d75bf42_1587619728489',
            results
        );

        const queryResponse = await requester
            .post(`/api/v1/document/query/csv/${datasetId}?sql=${encodeURI(query)}`)
            .send(requestBody);

        queryResponse.status.should.equal(200);
        queryResponse.body.should.have.property('data').and.be.an('array');
        queryResponse.body.should.have.property('meta').and.be.an('object');

        queryResponse.body.data.should.have.lengthOf(results.length);

        const resultList = queryResponse.body.data.map((elem) => ({
            thresh: elem.thresh,
            iso: elem.iso,
            adm1: elem.adm1,
            adm2: elem.adm2,
            area: elem.area
        }));

        resultList.should.deep.equal(results);
        (await hasOpenScrolls()).should.equal(false);
    });

    it('Query with special characters to dataset should be successful (happy case)', async () => {
        const datasetId = uuid.v4();

        createMockGetDataset(datasetId);

        const requestBody = {
            loggedUser: null
        };

        const query = `SELECT * FROM ${datasetId} WHERE iso = 'US&%'`;

        const results = [
            {
                thresh: 75, iso: 'US&%', adm1: 27, adm2: 1641, area: 41420.47960515353
            }, {
                thresh: 75, iso: 'BRA', adm1: 12, adm2: 1450, area: 315602.3928570104
            }, {
                thresh: 75, iso: 'RUS', adm1: 35, adm2: 925, area: 1137359.9711284428
            }, {
                thresh: 75, iso: 'COL', adm1: 30, adm2: 1017, area: 128570.48945388374
            }
        ];

        nock(process.env.CT_URL)
            .get(`/v1/convert/sql2SQL`)
            .query({
                sql: query
            })
            .once()
            .reply(200, {
                status: 200,
                data: {
                    type: 'result',
                    id: 'undefined',
                    attributes: {
                        query,
                        jsonSql: {
                            select: [
                                {
                                    value: '*',
                                    alias: null,
                                    type: 'wildcard'
                                }
                            ],
                            from: datasetId,
                            where: [
                                {
                                    type: 'operator',
                                    value: '=',
                                    left: [
                                        {
                                            value: 'iso',
                                            type: 'literal'
                                        }
                                    ],
                                    right: [
                                        {
                                            value: 'US&%',
                                            type: 'string'
                                        }
                                    ]
                                }
                            ]
                        }
                    },
                    relationships: {}
                }
            });

        await createIndex(
            'test_index_d1ced4227cd5480a8904d3410d75bf42_1587619728489',
            {
                adm1: { type: 'long' },
                adm2: { type: 'long' },
                area: { type: 'float' },
                iso: {
                    type: 'text',
                    fields: { keyword: { type: 'keyword', ignore_above: 256 } }
                },
                thresh: { type: 'long' }
            }
        );

        await insertData(
            'test_index_d1ced4227cd5480a8904d3410d75bf42_1587619728489',
            results
        );

        const queryResponse = await requester
            .post(`/api/v1/document/query/csv/${datasetId}`)
            .query({
                sql: query
            })
            .send(requestBody);

        queryResponse.status.should.equal(200);
        queryResponse.body.should.have.property('data').and.be.an('array');
        queryResponse.body.should.have.property('meta').and.be.an('object');

        queryResponse.body.data.should.have.lengthOf(1);

        delete queryResponse.body.data[0]._id;

        queryResponse.body.data[0].should.deep.equal({
            thresh: 75, iso: 'US&%', adm1: 27, adm2: 1641, area: 41420.47960515353
        });
        (await hasOpenScrolls()).should.equal(false);
    });

    it('Query with alias (x AS y) in SELECT clause should be successful (happy case)', async () => {
        const datasetId = uuid.v4();

        createMockGetDataset(datasetId);

        const requestBody = {
            loggedUser: null
        };

        const query = `SELECT year_data.year AS year FROM ${datasetId}'`;

        const results = [
            {
                year_data: [
                    { year: 2001 },
                    { year: 2002 },
                    { year: 2003 },
                    { year: 2004 },
                    { year: 2005 },
                    { year: 2006 },
                    { year: 2007 },
                    { year: 2008 },
                    { year: 2009 },
                    { year: 2010 },
                    { year: 2011 },
                    { year: 2012 },
                    { year: 2013 },
                    { year: 2014 },
                    { year: 2015 },
                    { year: 2016 },
                    { year: 2017 }
                ]
            }, {
                year_data: [
                    { year: 2001 },
                    { year: 2002 },
                    { year: 2003 },
                    { year: 2004 },
                    { year: 2005 },
                    { year: 2006 },
                    { year: 2007 },
                    { year: 2008 },
                    { year: 2009 },
                    { year: 2010 },
                    { year: 2011 },
                    { year: 2012 },
                    { year: 2013 },
                    { year: 2014 },
                    { year: 2015 },
                    { year: 2016 },
                    { year: 2017 }
                ]
            },
            {
                year_data: [
                    { year: 2001 },
                    { year: 2002 },
                    { year: 2003 },
                    { year: 2004 },
                    { year: 2005 },
                    { year: 2006 },
                    { year: 2007 },
                    { year: 2008 },
                    { year: 2009 },
                    { year: 2010 },
                    { year: 2011 },
                    { year: 2012 },
                    { year: 2013 },
                    { year: 2014 },
                    { year: 2015 },
                    { year: 2016 },
                    { year: 2017 }
                ]
            }
        ];

        await createIndex(
            'test_index_d1ced4227cd5480a8904d3410d75bf42_1587619728489',
            {
                year_data: {
                    properties: {
                        year: { type: 'long' }
                    }
                }
            }
        );

        await insertData(
            'test_index_d1ced4227cd5480a8904d3410d75bf42_1587619728489',
            results
        );

        nock(process.env.CT_URL)
            .get('/v1/convert/sql2SQL')
            .query({ sql: query })
            .reply(200, {
                data: {
                    type: 'result',
                    id: 'undefined',
                    attributes: {
                        query: 'SELECT year_data.year AS year FROM test_index_d1ced4227cd5480a8904d3410d75bf42_1587619728489',
                        jsonSql: {
                            select: [
                                {
                                    value: 'year_data',
                                    alias: 'year',
                                    type: 'literal'
                                }, {
                                    type: 'dot'
                                }, {
                                    value: 'year',
                                    alias: 'year',
                                    type: 'literal'
                                }],
                            from: 'test_index_d1ced4227cd5480a8904d3410d75bf42_1587619728489'
                        }
                    },
                    relationships: {}
                }
            });

        const queryResponse = await requester
            .post(`/api/v1/document/query/csv/${datasetId}?sql=${encodeURI(query)}`)
            .send(requestBody);

        queryResponse.status.should.equal(200);
        queryResponse.body.should.have.property('data').and.be.an('array');
        queryResponse.body.should.have.property('meta').and.be.an('object');

        queryResponse.body.data.should.have.lengthOf(results.length);

        const responseResults = queryResponse.body.data.map((e) => ({
            year_data: e.year_data
        }));

        responseResults.should.deep.equal(results);
        (await hasOpenScrolls()).should.equal(false);
    });

    it('Query with order by mapping from Elasticsearch should be successful (happy case)', async () => {
        const datasetId = uuid.v4();

        createMockGetDataset(datasetId);

        const requestBody = {
            loggedUser: null
        };

        const query = `SELECT numeric_m as x, A1B_2080s as y, month as z from ${datasetId} where streamflow = 'Snohomish' order by numeric_m ASC`;

        const results = [
            {
                numeric_m: 1,
                month: 'Oct',
                A1B_2080s: 6933.430769
            },
            {
                numeric_m: 2,
                month: 'Nov',
                A1B_2080s: 15743.70549
            },
            {
                numeric_m: 3,
                month: 'Dec',
                A1B_2080s: 20168.46484
            },
            {
                numeric_m: 4,
                month: 'Jan',
                A1B_2080s: 19139.62198
            }
        ];

        await createIndex(
            'test_index_d1ced4227cd5480a8904d3410d75bf42_1587619728489',
            {
                A1B_2040s: {
                    type: 'double'
                },
                A1B_2080s: {
                    type: 'double'
                },
                historical: {
                    type: 'long'
                },
                month: {
                    type: 'text'
                },
                numeric_m: {
                    type: 'long'
                },
                streamflow: {
                    type: 'keyword'
                }
            }
        );

        await insertData(
            'test_index_d1ced4227cd5480a8904d3410d75bf42_1587619728489',
            results.map((elem, index) => ({
                A1B_2040s: 123.456,
                A1B_2080s: elem.A1B_2080s,
                historical: 2345678,
                month: elem.month,
                numeric_m: index + 1,
                streamflow: 'Snohomish'
            }))
        );

        nock(process.env.CT_URL)
            .get('/v1/convert/sql2SQL')
            .query({ sql: query })
            .reply(200, {
                data: {
                    type: 'result',
                    attributes: {
                        query: 'SELECT numeric_m AS x, A1B_2080s AS y, month AS z FROM test_index_d1ced4227cd5480a8904d3410d75bf42_1587619728489 WHERE streamflow = \'Snohomish\' ORDER BY numeric_m ASC',
                        jsonSql: {
                            select: [
                                {
                                    value: 'numeric_m',
                                    alias: 'x',
                                    type: 'literal'
                                },
                                {
                                    value: 'A1B_2080s',
                                    alias: 'y',
                                    type: 'literal'
                                },
                                {
                                    value: 'month',
                                    alias: 'z',
                                    type: 'literal'
                                }
                            ],
                            from: 'test_index_d1ced4227cd5480a8904d3410d75bf42_1587619728489',
                            where: [
                                {
                                    type: 'operator',
                                    value: '=',
                                    left: [
                                        {
                                            value: 'streamflow',
                                            type: 'literal'
                                        }
                                    ],
                                    right: [
                                        {
                                            value: 'Snohomish',
                                            type: 'string'
                                        }
                                    ]
                                }
                            ],
                            orderBy: [
                                {
                                    type: 'literal',
                                    value: 'numeric_m',
                                    alias: null,
                                    direction: 'ASC'
                                }
                            ]
                        }
                    }
                }
            });

        const queryResponse = await requester
            .post(`/api/v1/document/query/csv/${datasetId}`)
            .query({
                sql: query
            })
            .send(requestBody);

        queryResponse.status.should.equal(200);
        queryResponse.body.should.have.property('data').and.be.an('array');
        queryResponse.body.should.have.property('meta').and.be.an('object');

        queryResponse.body.data.should.have.lengthOf(results.length);

        const resultList = [
            {
                numeric_m: 1,
                month: 'Oct',
                A1B_2080s: 6933.430769,
                x: 1,
                y: 6933.430769,
                z: 'Oct'
            },
            {
                numeric_m: 2,
                month: 'Nov',
                A1B_2080s: 15743.70549,
                x: 2,
                y: 15743.70549,
                z: 'Nov'
            },
            {
                numeric_m: 3,
                month: 'Dec',
                A1B_2080s: 20168.46484,
                x: 3,
                y: 20168.46484,
                z: 'Dec'
            },
            {
                numeric_m: 4,
                month: 'Jan',
                A1B_2080s: 19139.62198,
                x: 4,
                y: 19139.62198,
                z: 'Jan'
            }
        ];

        const responseResults = queryResponse.body.data.map((e) => ({
            numeric_m: e.numeric_m,
            month: e.month,
            A1B_2080s: e.A1B_2080s,
            x: e.x,
            y: e.y,
            z: e.z,
        }));

        responseResults.should.deep.equal(resultList);
        (await hasOpenScrolls()).should.equal(false);
    });

    afterEach(async () => {
        await deleteTestIndeces();

        if (!nock.isDone()) {
            const pendingMocks = nock.pendingMocks();
            throw new Error(`Not all nock interceptors were used: ${pendingMocks}`);
        }
    });
});

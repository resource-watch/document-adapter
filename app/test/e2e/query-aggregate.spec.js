/* eslint-disable max-len */
const nock = require('nock');
const chai = require('chai');
const { getTestServer } = require('./utils/test-server');
const {
    createMockGetDataset, createIndex, deleteTestIndeces, insertData
} = require('./utils/helpers');

chai.should();

const requester = getTestServer();

nock.disableNetConnect();
nock.enableNetConnect((host) => [`${process.env.HOST_IP}:${process.env.PORT}`, process.env.ELASTIC_TEST_URL].includes(host));

describe('Query datasets - Aggregate queries', () => {

    before(async () => {
        if (process.env.NODE_ENV !== 'test') {
            throw Error(`Running the test suite with NODE_ENV ${process.env.NODE_ENV} may result in permanent data loss. Please use NODE_ENV=test.`);
        }
    });

    beforeEach(async () => {
        await deleteTestIndeces();
    });

    it('Query an empty dataset should be successful (happy case)', async () => {
        const datasetId = new Date().getTime();

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
            .post(`/api/v1/document/query/${datasetId}?sql=${encodeURI(query)}`)
            .send(requestBody);

        queryResponse.status.should.equal(200);
        queryResponse.body.should.have.property('data').and.be.an('array');
        queryResponse.body.should.have.property('meta').and.be.an('object');

        queryResponse.body.data.should.have.lengthOf(0);
    });

    it('count(*) query to dataset should be successful (happy case)', async () => {
        const datasetId = new Date().getTime();

        createMockGetDataset(datasetId);

        const requestBody = {
            loggedUser: null
        };

        const query = `select count (*) from ${datasetId}`;

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
                data: {
                    type: 'result',
                    attributes: {
                        query: `SELECT count(*) FROM ${datasetId}`,
                        jsonSql: {
                            select: [
                                {
                                    type: 'function',
                                    alias: null,
                                    value: 'count',
                                    arguments: [
                                        {
                                            value: '*',
                                            alias: null,
                                            type: 'wildcard'
                                        }
                                    ]
                                }
                            ],
                            from: datasetId
                        }
                    }
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
            .post(`/api/v1/document/query/${datasetId}?sql=${encodeURI(query)}`)
            .send(requestBody);

        queryResponse.status.should.equal(200);
        queryResponse.body.should.have.property('data').and.be.an('array');
        queryResponse.body.should.have.property('meta').and.be.an('object');

        queryResponse.body.data.should.have.lengthOf(1);
        queryResponse.body.data[0].should.have.property('count').and.equal(results.length);
    });

    it('Query with multiple aggregations to dataset should be successful (happy case)', async () => {
        const datasetId = new Date().getTime();

        createMockGetDataset(datasetId);

        const requestBody = {
            loggedUser: null
        };

        const query = `select count(*), avg(area), sum(area), min(area), max(area) from ${datasetId}`;

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
                data: {
                    type: 'result',
                    attributes: {
                        query: `SELECT count(*), avg(area), sum(area), min(area), max(area) FROM ${datasetId}`,
                        jsonSql: {
                            select: [
                                {
                                    type: 'function',
                                    alias: null,
                                    value: 'count',
                                    arguments: [
                                        {
                                            value: '*',
                                            alias: null,
                                            type: 'wildcard'
                                        }
                                    ]
                                },
                                {
                                    type: 'function',
                                    alias: null,
                                    value: 'avg',
                                    arguments: [
                                        {
                                            value: 'area',
                                            type: 'literal'
                                        }
                                    ]
                                },
                                {
                                    type: 'function',
                                    alias: null,
                                    value: 'sum',
                                    arguments: [
                                        {
                                            value: 'area',
                                            type: 'literal'
                                        }
                                    ]
                                },
                                {
                                    type: 'function',
                                    alias: null,
                                    value: 'min',
                                    arguments: [
                                        {
                                            value: 'area',
                                            type: 'literal'
                                        }
                                    ]
                                },
                                {
                                    type: 'function',
                                    alias: null,
                                    value: 'max',
                                    arguments: [
                                        {
                                            value: 'area',
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
            .post(`/api/v1/document/query/${datasetId}?sql=${encodeURI(query)}`)
            .send(requestBody);

        queryResponse.status.should.equal(200);
        queryResponse.body.should.have.property('data').and.be.an('array');
        queryResponse.body.should.have.property('meta').and.be.an('object');

        queryResponse.body.data.should.have.lengthOf(1);
        queryResponse.body.data[0].should.have.property('count').and.equal(results.length);
        queryResponse.body.data[0].should.have.property('avg').and.equal(405738.3447265625);
        queryResponse.body.data[0].should.have.property('min').and.equal(41420.48046875);
        queryResponse.body.data[0].should.have.property('max').and.equal(1137360);
        queryResponse.body.data[0].should.have.property('sum').and.equal(1622953.37890625);
    });

    it('Query with nested aggregations to dataset should be successful (happy case)', async () => {
        const datasetId = new Date().getTime();

        createMockGetDataset(datasetId);

        const requestBody = {
            loggedUser: null
        };

        const query = `select count(*) from ${datasetId} GROUP BY iso, adm1`;

        const results = [
            {
                thresh: 75, iso: 'USA', adm1: 27, adm2: 1641, area: 41420.47960515353
            }, {
                thresh: 75, iso: 'BRA', adm1: 27, adm2: 1450, area: 315602.3928570104
            }, {
                thresh: 75, iso: 'RUS', adm1: 35, adm2: 925, area: 1137359.9711284428
            }, {
                thresh: 75, iso: 'COL', adm1: 35, adm2: 1017, area: 128570.48945388374
            }, {
                thresh: 75, iso: 'COL', adm1: 12, adm2: 1017, area: 128570.48945388374
            }, {
                thresh: 75, iso: 'COL', adm1: 12, adm2: 1017, area: 128570.48945388374
            }
        ];

        nock(process.env.CT_URL)
            .get(`/v1/convert/sql2SQL`)
            .query({
                sql: query
            })
            .once()
            .reply(200, {
                data: {
                    type: 'result',
                    attributes: {
                        query: `SELECT count(*) FROM ${datasetId} GROUP BY iso, adm1`,
                        jsonSql: {
                            select: [
                                {
                                    type: 'function',
                                    alias: null,
                                    value: 'count',
                                    arguments: [
                                        {
                                            value: '*',
                                            alias: null,
                                            type: 'wildcard'
                                        }
                                    ]
                                }
                            ],
                            from: datasetId,
                            group: [
                                {
                                    type: 'literal',
                                    value: 'iso'
                                },
                                {
                                    type: 'literal',
                                    value: 'adm1'
                                }
                            ]
                        }
                    }
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
            .post(`/api/v1/document/query/${datasetId}?sql=${encodeURI(query)}`)
            .send(requestBody);

        queryResponse.status.should.equal(200);
        queryResponse.body.should.have.property('data').and.be.an('array');
        queryResponse.body.should.have.property('meta').and.be.an('object');

        queryResponse.body.data.should.deep.equal([
            {
                adm1: 12,
                count: 2,
                iso: 'COL'
            },
            {
                adm1: 35,
                count: 1,
                iso: 'COL'
            },
            {
                adm1: 27,
                count: 1,
                iso: 'BRA'
            },
            {
                adm1: 35,
                count: 1,
                iso: 'RUS'
            },
            {
                adm1: 27,
                count: 1,
                iso: 'USA'
            }
        ]);
    });

    it('Query with alias in upper case (x AS Y) in aggregation should be successful', async () => {
        const datasetId = new Date().getTime();

        createMockGetDataset(datasetId);

        const requestBody = {
            loggedUser: null
        };

        const query = `SELECT min(year) AS value_Year FROM ${datasetId} GROUP BY year'`;

        const results = [
            { year: 2001 },
            { year: 2001 },
            { year: 2001 },
            { year: 2001 },
            { year: 2002 },
            { year: 2002 },
            { year: 2002 },
            { year: 2002 },
            { year: 2002 },
            { year: 2003 },
            { year: 2003 },
            { year: 2003 },
            { year: 2003 },
            { year: 2003 },
            { year: 2003 },
            { year: 2004 },
            { year: 2004 },
            { year: 2004 },
            { year: 2017 }
        ];

        await createIndex(
            'test_index_d1ced4227cd5480a8904d3410d75bf42_1587619728489',
            {
                year: { type: 'long' }
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
                    attributes: {
                        query: `SELECT min(year) AS value_Year FROM ${datasetId} GROUP BY year`,
                        jsonSql: {
                            select: [
                                {
                                    type: 'function',
                                    alias: 'value_Year',
                                    value: 'min',
                                    arguments: [
                                        {
                                            value: 'year',
                                            type: 'literal'
                                        }
                                    ]
                                }
                            ],
                            from: datasetId,
                            group: [
                                {
                                    type: 'literal',
                                    value: 'year'
                                }
                            ]
                        }
                    }
                }
            });

        const queryResponse = await requester
            .post(`/api/v1/document/query/${datasetId}?sql=${encodeURI(query)}`)
            .send(requestBody);

        queryResponse.status.should.equal(200);
        queryResponse.body.should.have.property('data').and.be.an('array');
        queryResponse.body.should.have.property('meta').and.be.an('object');

        queryResponse.body.data.should.deep.equal([
            {
                value_Year: 2003,
                year: 2003
            },
            {
                value_Year: 2002,
                year: 2002
            },
            {
                value_Year: 2001,
                year: 2001
            },
            {
                value_Year: 2004,
                year: 2004
            },
            {
                value_Year: 2017,
                year: 2017
            }
        ]);
    });

    it('Query with nested aggregations and aliases to dataset should be successful (happy case)', async () => {
        const datasetId = new Date().getTime();

        createMockGetDataset(datasetId);

        const requestBody = {
            loggedUser: null
        };

        const query = `select count(*) as the_count, iso as the_iso from ${datasetId} GROUP BY iso, adm1`;

        const results = [
            {
                thresh: 75, iso: 'USA', adm1: 27, adm2: 1641, area: 41420.47960515353
            }, {
                thresh: 75, iso: 'BRA', adm1: 27, adm2: 1450, area: 315602.3928570104
            }, {
                thresh: 75, iso: 'RUS', adm1: 35, adm2: 925, area: 1137359.9711284428
            }, {
                thresh: 75, iso: 'COL', adm1: 35, adm2: 1017, area: 128570.48945388374
            }, {
                thresh: 75, iso: 'COL', adm1: 12, adm2: 1017, area: 128570.48945388374
            }, {
                thresh: 75, iso: 'COL', adm1: 12, adm2: 1017, area: 128570.48945388374
            }
        ];

        nock(process.env.CT_URL)
            .get(`/v1/convert/sql2SQL`)
            .query({
                sql: query
            })
            .once()
            .reply(200, {
                data: {
                    type: 'result',
                    attributes: {
                        query: `SELECT count(*) AS the_count, iso AS the_iso FROM {datasetId} GROUP BY iso, adm1`,
                        jsonSql: {
                            select: [
                                {
                                    type: 'function',
                                    alias: 'the_count',
                                    value: 'count',
                                    arguments: [
                                        {
                                            value: '*',
                                            alias: null,
                                            type: 'wildcard'
                                        }
                                    ]
                                },
                                {
                                    value: 'iso',
                                    alias: 'the_iso',
                                    type: 'literal'
                                }
                            ],
                            from: datasetId,
                            group: [
                                {
                                    type: 'literal',
                                    value: 'iso'
                                },
                                {
                                    type: 'literal',
                                    value: 'adm1'
                                }
                            ]
                        }
                    }
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
            .post(`/api/v1/document/query/${datasetId}?sql=${encodeURI(query)}`)
            .send(requestBody);

        queryResponse.status.should.equal(200);
        queryResponse.body.should.have.property('data').and.be.an('array');
        queryResponse.body.should.have.property('meta').and.be.an('object');

        queryResponse.body.data.should.deep.equal([
            {
                adm1: 12,
                the_count: 2,
                iso: 'COL',
                the_iso: 'COL'
            },
            {
                adm1: 35,
                the_count: 1,
                iso: 'COL',
                the_iso: 'COL'
            },
            {
                adm1: 27,
                the_count: 1,
                iso: 'BRA',
                the_iso: 'BRA'
            },
            {
                adm1: 35,
                the_count: 1,
                iso: 'RUS',
                the_iso: 'RUS'
            },
            {
                adm1: 27,
                the_count: 1,
                iso: 'USA',
                the_iso: 'USA'
            }
        ]);
    });

    afterEach(async () => {
        await deleteTestIndeces();

        if (!nock.isDone()) {
            const pendingMocks = nock.pendingMocks();
            if (pendingMocks.length > 1) {
                throw new Error(`Not all nock interceptors were used: ${pendingMocks}`);
            }
        }
    });
});

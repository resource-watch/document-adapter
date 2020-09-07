/* eslint-disable max-len */
const nock = require('nock');
const config = require('config');
const chai = require('chai');
const { getTestServer } = require('./utils/test-server');
const { createMockGetDataset } = require('./utils/helpers');

chai.should();

const requester = getTestServer();

const elasticUri = config.get('elasticsearch.host');

nock.disableNetConnect();
nock.enableNetConnect(`${process.env.HOST_IP}:${process.env.PORT}`);

describe('Query datasets - GROUP BY queries', () => {

    before(async () => {
        if (process.env.NODE_ENV !== 'test') {
            throw Error(`Running the test suite with NODE_ENV ${process.env.NODE_ENV} may result in permanent data loss. Please use NODE_ENV=test.`);
        }
    });

    it('Group by `column` query to dataset should be successful (happy case)', async () => {
        const timestamp = new Date().getTime();

        createMockGetDataset(timestamp);

        const requestBody = {
            loggedUser: null
        };

        const query = `select * from ${timestamp} group by field`;

        const results = [
            { key: 'ARM', doc_count: 1 }, { key: 'AUS', doc_count: 1 }, {
                key: 'AZE',
                doc_count: 1
            }, { key: 'BDI', doc_count: 1 }, { key: 'BIH', doc_count: 1 }, {
                key: 'BOL',
                doc_count: 1
            }, { key: 'BRA', doc_count: 1 }, { key: 'BTN', doc_count: 1 }, {
                key: 'CAN',
                doc_count: 1
            }, { key: 'CHE', doc_count: 1 }, { key: 'CHN', doc_count: 1 }, {
                key: 'CIV',
                doc_count: 1
            }, { key: 'CMR', doc_count: 1 }, { key: 'CRI', doc_count: 1 }, {
                key: 'ECU',
                doc_count: 1
            }, { key: 'GNB', doc_count: 1 }, { key: 'GRD', doc_count: 1 }, {
                key: 'HTI',
                doc_count: 1
            }, { key: 'IDN', doc_count: 1 }, { key: 'IRQ', doc_count: 1 }, {
                key: 'KAZ',
                doc_count: 1
            }, { key: 'KGZ', doc_count: 1 }, { key: 'KNA', doc_count: 1 }, {
                key: 'LBN',
                doc_count: 1
            }, { key: 'LIE', doc_count: 1 }, { key: 'LSO', doc_count: 1 }, {
                key: 'MAR',
                doc_count: 1
            }, { key: 'MEX', doc_count: 1 }, { key: 'MMR', doc_count: 1 }, {
                key: 'MOZ',
                doc_count: 1
            }, { key: 'MUS', doc_count: 1 }, { key: 'NER', doc_count: 1 }, {
                key: 'NGA',
                doc_count: 1
            }, { key: 'NZL', doc_count: 1 }, { key: 'PER', doc_count: 1 }, {
                key: 'PNG',
                doc_count: 1
            }, { key: 'PRK', doc_count: 1 }, { key: 'PRY', doc_count: 1 }, {
                key: 'RUS',
                doc_count: 1
            }, { key: 'RWA', doc_count: 1 }, { key: 'SDN', doc_count: 1 }, {
                key: 'SLE',
                doc_count: 1
            }, { key: 'SLV', doc_count: 1 }, { key: 'TGO', doc_count: 1 }, {
                key: 'TJK',
                doc_count: 1
            }, { key: 'UGA', doc_count: 1 }, { key: 'UKR', doc_count: 1 }, {
                key: 'USA',
                doc_count: 1
            }, { key: 'VCT', doc_count: 1 }, { key: 'VNM', doc_count: 1 }, { key: 'YEM', doc_count: 1 }];

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
                        query: `SELECT * FROM ${timestamp}`,
                        jsonSql: {
                            select: [
                                {
                                    value: '*',
                                    alias: null,
                                    type: 'wildcard'
                                }
                            ],
                            from: timestamp,
                            group: [
                                {
                                    type: 'literal',
                                    value: 'ISO'
                                }
                            ]
                        }
                    },
                    relationships: {}
                }
            });

        nock(elasticUri)
            .get('/index_d1ced4227cd5480a8904d3410d75bf42_1587619728489/_mapping')
            .reply(200, {
                index_d1ced4227cd5480a8904d3410d75bf42_1587619728489: {
                    mappings: {
                        _doc: {
                            properties: {
                                Country: { type: 'text', fields: { keyword: { type: 'keyword', ignore_above: 256 } } },
                                'INDC-vs-NDC': {
                                    type: 'text',
                                    fields: { keyword: { type: 'keyword', ignore_above: 256 } }
                                },
                                ISO: { type: 'text', fields: { keyword: { type: 'keyword', ignore_above: 256 } } },
                                data_id: { type: 'text', fields: { keyword: { type: 'keyword', ignore_above: 256 } } },
                                'economyWide-Target': {
                                    type: 'text',
                                    fields: { keyword: { type: 'keyword', ignore_above: 256 } }
                                },
                                'economyWide-Target-Description': {
                                    type: 'text',
                                    fields: { keyword: { type: 'keyword', ignore_above: 256 } }
                                },
                                'landUse-GHG': { type: 'long' },
                                'landUse-GHG-Description': { type: 'long' },
                                'landUse-NonGHG': {
                                    type: 'text',
                                    fields: { keyword: { type: 'keyword', ignore_above: 256 } }
                                },
                                'landUse-NonGHG-Description': {
                                    type: 'text',
                                    fields: { keyword: { type: 'keyword', ignore_above: 256 } }
                                },
                                'landuse-excluded': { type: 'long' },
                                'landuse-excluded-description': { type: 'long' }
                            }
                        }
                    }
                }
            }, ['content-type',
                'application/json; charset=UTF-8',
                'content-length',
                '989']);

        nock(elasticUri)
            .get('/index_d1ced4227cd5480a8904d3410d75bf42_1587619728489/_mapping')
            .reply(200, {
                index_d1ced4227cd5480a8904d3410d75bf42_1587619728489: {
                    mappings: {
                        _doc: {
                            properties: {
                                Country: { type: 'text', fields: { keyword: { type: 'keyword', ignore_above: 256 } } },
                                'INDC-vs-NDC': {
                                    type: 'text',
                                    fields: { keyword: { type: 'keyword', ignore_above: 256 } }
                                },
                                ISO: { type: 'text', fields: { keyword: { type: 'keyword', ignore_above: 256 } } },
                                data_id: { type: 'text', fields: { keyword: { type: 'keyword', ignore_above: 256 } } },
                                'economyWide-Target': {
                                    type: 'text',
                                    fields: { keyword: { type: 'keyword', ignore_above: 256 } }
                                },
                                'economyWide-Target-Description': {
                                    type: 'text',
                                    fields: { keyword: { type: 'keyword', ignore_above: 256 } }
                                },
                                'landUse-GHG': { type: 'long' },
                                'landUse-GHG-Description': { type: 'long' },
                                'landUse-NonGHG': {
                                    type: 'text',
                                    fields: { keyword: { type: 'keyword', ignore_above: 256 } }
                                },
                                'landUse-NonGHG-Description': {
                                    type: 'text',
                                    fields: { keyword: { type: 'keyword', ignore_above: 256 } }
                                },
                                'landuse-excluded': { type: 'long' },
                                'landuse-excluded-description': { type: 'long' }
                            }
                        }
                    }
                }
            }, ['content-type',
                'application/json; charset=UTF-8',
                'content-length',
                '989']);

        nock(elasticUri)
            .post('/_opendistro/_sql/_explain')
            .reply(200, {
                from: 0,
                size: 0,
                aggregations: {
                    'ISO.keyword': {
                        terms: {
                            field: 'ISO.keyword',
                            size: 9999999,
                            min_doc_count: 1,
                            shard_min_doc_count: 0,
                            show_term_doc_count_error: false,
                            order: [{ _count: 'desc' }, { _term: 'asc' }]
                        }
                    }
                }
            }, ['content-type',
                'text/plain; charset=UTF-8',
                'content-length',
                '415']);

        nock(elasticUri)
            .post('/index_d1ced4227cd5480a8904d3410d75bf42_1587619728489/_search')
            .query({ scroll: '1m' })
            .reply(200, {
                _scroll_id: 'DnF1ZXJ5VGhlbkZldGNoBQAAAAAAAAAMFkl1WXZDWUJfU3ZtRUo5OVZwOEVaNncAAAAAAAAACxZJdVl2Q1lCX1N2bUVKOTlWcDhFWjZ3AAAAAAAAAA0WSXVZdkNZQl9Tdm1FSjk5VnA4RVo2dwAAAAAAAAAOFkl1WXZDWUJfU3ZtRUo5OVZwOEVaNncAAAAAAAAADxZJdVl2Q1lCX1N2bUVKOTlWcDhFWjZ3',
                took: 5,
                timed_out: false,
                _shards: { total: 5, successful: 5, failed: 0 },
                hits: { total: 51, max_score: 0, hits: [] },
                aggregations: {
                    'ISO.keyword': {
                        doc_count_error_upper_bound: 0,
                        sum_other_doc_count: 0,
                        buckets: results
                    }
                }
            }, ['content-type',
                'application/json; charset=UTF-8',
                'content-length',
                '1895']);

        const queryResponse = await requester
            .post(`/api/v1/document/query/${timestamp}?sql=${query}`)
            .send(requestBody);

        queryResponse.status.should.equal(200);
        queryResponse.body.should.have.property('data').and.be.an('array');
        queryResponse.body.should.have.property('meta').and.be.an('object');

        queryResponse.body.data.should.have.lengthOf(results.length);

        const resultList = results.map((elem) => ({ ISO: elem.key }));

        queryResponse.body.data.should.deep.equal(resultList);
    });

    it('Group by date part query to dataset should be successful', async () => {
        const timestamp = new Date().getTime();

        createMockGetDataset(timestamp);

        const requestBody = {
            loggedUser: null
        };

        const query = `select createdAt from ${timestamp} group by date_histogram('field'="createdAt",'interval'='1d')`;

        const results = [
            {
                'date_histogram(field=createdAt,interval=1d)': 1536278400000
            },
            {
                'date_histogram(field=createdAt,interval=1d)': 1536364800000
            },
            {
                'date_histogram(field=createdAt,interval=1d)': 1536451200000
            },
            {
                'date_histogram(field=createdAt,interval=1d)': 1536537600000
            },
            {
                'date_histogram(field=createdAt,interval=1d)': 1536624000000
            },
            {
                'date_histogram(field=createdAt,interval=1d)': 1536710400000
            },
            {
                'date_histogram(field=createdAt,interval=1d)': 1536796800000
            },
            {
                'date_histogram(field=createdAt,interval=1d)': 1536883200000
            },
            {
                'date_histogram(field=createdAt,interval=1d)': 1536969600000
            },
            {
                'date_histogram(field=createdAt,interval=1d)': 1537056000000
            },
            {
                'date_histogram(field=createdAt,interval=1d)': 1537142400000
            }];

        nock(process.env.CT_URL)
            .get('/v1/convert/sql2SQL')
            .query({ sql: query })
            .reply(200, {
                data: {
                    type: 'result',
                    id: 'undefined',
                    attributes: {
                        query: `SELECT createdAt FROM ${timestamp} GROUP BY date_histogram( 'field'="createdAt", 'interval'='1d')`,
                        jsonSql: {
                            select: [{ value: 'createdAt', alias: null, type: 'literal' }],
                            from: timestamp,
                            group: [{
                                type: 'function',
                                alias: null,
                                value: 'date_histogram',
                                arguments: [{
                                    name: 'field',
                                    type: 'literal',
                                    value: 'createdAt'
                                }, { name: 'interval', type: 'string', value: '1d' }]
                            }]
                        }
                    },
                    relationships: {}
                }
            }, ['Vary',
                'Origin',
                'Content-Type',
                'application/vnd.api+json',
                'X-Response-Time',
                '17 ms',
                'Content-Length',
                '517',
                'Date',
                'Tue, 25 Sep 2018 06:32:01 GMT',
                'Connection',
                'close']);

        nock(elasticUri)
            .get('/index_d1ced4227cd5480a8904d3410d75bf42_1587619728489/_mapping')
            .reply(200, {
                index_d1ced4227cd5480a8904d3410d75bf42_1587619728489: {
                    mappings: {
                        _doc: {
                            properties: {
                                createdAt: { type: 'date' },
                                profilePictureUrl: {
                                    type: 'text',
                                    fields: { keyword: { type: 'keyword', ignore_above: 256 } }
                                },
                                screenName: {
                                    type: 'text',
                                    fields: { keyword: { type: 'keyword', ignore_above: 256 } }
                                },
                                text: {
                                    type: 'text',
                                    fields: { keyword: { type: 'keyword', ignore_above: 256 } }
                                }
                            }
                        }
                    }
                }
            }, ['content-type',
                'application/json; charset=UTF-8',
                'content-length',
                '388']);

        nock(elasticUri, { encodedQueryParams: true })
            .get('/index_d1ced4227cd5480a8904d3410d75bf42_1587619728489/_mapping')
            .reply(200, {
                index_d1ced4227cd5480a8904d3410d75bf42_1587619728489: {
                    mappings: {
                        _doc: {
                            properties: {
                                createdAt: { type: 'date' },
                                profilePictureUrl: {
                                    type: 'text',
                                    fields: { keyword: { type: 'keyword', ignore_above: 256 } }
                                },
                                screenName: {
                                    type: 'text',
                                    fields: { keyword: { type: 'keyword', ignore_above: 256 } }
                                },
                                text: {
                                    type: 'text',
                                    fields: { keyword: { type: 'keyword', ignore_above: 256 } }
                                }
                            }
                        }
                    }
                }
            }, ['content-type',
                'application/json; charset=UTF-8',
                'content-length',
                '388']);

        nock(elasticUri)
            .post('/_opendistro/_sql/_explain')
            .reply(200, {
                from: 0,
                size: 0,
                _source: { includes: ['createdAt'], excludes: [] },
                stored_fields: 'createdAt',
                aggregations: {
                    'date_histogram(field=createdAt,interval=1d)': {
                        date_histogram: {
                            field: 'createdAt',
                            format: 'yyyy-MM-dd HH:mm:ss',
                            interval: '1d',
                            offset: 0,
                            order: { _key: 'asc' },
                            keyed: false,
                            min_doc_count: 0
                        }
                    }
                }
            }, ['content-type',
                'text/plain; charset=UTF-8',
                'content-length',
                '501']);

        nock(elasticUri, { encodedQueryParams: true })
            .post('/index_d1ced4227cd5480a8904d3410d75bf42_1587619728489/_search')
            .query({ scroll: '1m' })
            .reply(200, {
                _scroll_id: 'DnF1ZXJ5VGhlbkZldGNoBQAAAAAAAAAVFkl1WXZDWUJfU3ZtRUo5OVZwOEVaNncAAAAAAAAAFxZJdVl2Q1lCX1N2bUVKOTlWcDhFWjZ3AAAAAAAAABYWSXVZdkNZQl9Tdm1FSjk5VnA4RVo2dwAAAAAAAAAYFkl1WXZDWUJfU3ZtRUo5OVZwOEVaNncAAAAAAAAAGRZJdVl2Q1lCX1N2bUVKOTlWcDhFWjZ3',
                took: 1,
                timed_out: false,
                _shards: { total: 5, successful: 5, failed: 0 },
                hits: { total: 20, max_score: 0, hits: [] },
                aggregations: {
                    'date_histogram(field=createdAt,interval=1d)': {
                        buckets: [{
                            key_as_string: '2018-09-07 00:00:00',
                            key: 1536278400000,
                            doc_count: 4
                        }, {
                            key_as_string: '2018-09-08 00:00:00',
                            key: 1536364800000,
                            doc_count: 0
                        }, {
                            key_as_string: '2018-09-09 00:00:00',
                            key: 1536451200000,
                            doc_count: 0
                        }, {
                            key_as_string: '2018-09-10 00:00:00',
                            key: 1536537600000,
                            doc_count: 1
                        }, {
                            key_as_string: '2018-09-11 00:00:00',
                            key: 1536624000000,
                            doc_count: 2
                        }, {
                            key_as_string: '2018-09-12 00:00:00',
                            key: 1536710400000,
                            doc_count: 3
                        }, {
                            key_as_string: '2018-09-13 00:00:00',
                            key: 1536796800000,
                            doc_count: 0
                        }, {
                            key_as_string: '2018-09-14 00:00:00',
                            key: 1536883200000,
                            doc_count: 5
                        }, {
                            key_as_string: '2018-09-15 00:00:00',
                            key: 1536969600000,
                            doc_count: 0
                        }, {
                            key_as_string: '2018-09-16 00:00:00',
                            key: 1537056000000,
                            doc_count: 0
                        }, { key_as_string: '2018-09-17 00:00:00', key: 1537142400000, doc_count: 5 }]
                    }
                }
            }, ['content-type',
                'application/json; charset=UTF-8',
                'content-length',
                '1257']);

        const queryResponse = await requester
            .post(`/api/v1/document/query/${timestamp}?sql=${query}`)
            .send(requestBody);

        queryResponse.status.should.equal(200);
        queryResponse.body.should.have.property('data').and.be.an('array');
        queryResponse.body.should.have.property('meta').and.be.an('object');

        queryResponse.body.data.should.have.lengthOf(results.length);

        queryResponse.body.data.should.deep.equal(results);
    });

    afterEach(() => {
        if (!nock.isDone()) {
            const pendingMocks = nock.pendingMocks();
            if (pendingMocks.length > 1) {
                throw new Error(`Not all nock interceptors were used: ${pendingMocks}`);
            }
        }

    });
});

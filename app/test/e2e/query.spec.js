/* eslint-disable no-unused-vars,no-undef */
const nock = require('nock');
const config = require('config');
const chai = require('chai');
const { getTestServer } = require('./test-server');

const should = chai.should();

const requester = getTestServer();

const elasticUri = process.env.ELASTIC_URI || `${config.get('elasticsearch.host')}:${config.get('elasticsearch.port')}`;

nock.disableNetConnect();
nock.enableNetConnect(`${process.env.HOST_IP}:${process.env.PORT}`);

describe('Dataset create tests', () => {

    before(async () => {
        if (process.env.NODE_ENV !== 'test') {
            throw Error(`Running the test suite with NODE_ENV ${process.env.NODE_ENV} may result in permanent data loss. Please use NODE_ENV=test.`);
        }

        nock.cleanAll();
    });

    it('Basic query to dataset should be successful (happy case)', async () => {
        const requestBody = {
            dataset: {
                name: 'Food Demand',
                slug: 'Food-Demand_3',
                type: null,
                subtitle: null,
                application: [
                    'rw'
                ],
                dataPath: 'data',
                attributesPath: null,
                connectorType: 'document',
                provider: 'json',
                userId: '1a10d7c6e0a37126611fd7a7',
                connectorUrl: 'http://gfw2-data.s3.amazonaws.com/alerts-tsv/output/to-api/output.json',
                tableName: 'index_051364f0fe4446c2bf95fa4b93e2dbd2_1536899613926',
                status: 'saved',
                published: true,
                overwrite: false,
                verified: false,
                blockchain: {},
                mainDateField: null,
                env: 'production',
                geoInfo: false,
                protected: false,
                legend: {
                    date: [],
                    region: [],
                    country: [],
                    nested: []
                },
                clonedHost: {},
                errorMessage: '',
                taskId: '/v1/doc-importer/task/986bd4ee-0bfe-4002-ae17-1d1594dffd0a',
                updatedAt: '2018-09-14T04:33:48.838Z',
                dataLastUpdated: null,
                widgetRelevantProps: [],
                layerRelevantProps: [],
                id: '051364f0-fe44-46c2-bf95-fa4b93e2dbd2'
            },
            loggedUser: null
        };

        const query = `select * from ${requestBody.dataset.id}`;

        const results = [
            {
                _index: 'index_051364f0fe4446c2bf95fa4b93e2dbd2_1536899613926',
                _type: 'type',
                _id: 'kDZb1mUBbvDJlUQCLHRu',
                _score: 1,
                _source: {
                    thresh: 75, iso: 'USA', adm1: 27, adm2: 1641, area: 41420.47960515353
                }
            }, {
                _index: 'index_051364f0fe4446c2bf95fa4b93e2dbd2_1536899613926',
                _type: 'type',
                _id: 'mzZb1mUBbvDJlUQCLHRu',
                _score: 1,
                _source: {
                    thresh: 75, iso: 'BRA', adm1: 12, adm2: 1450, area: 315602.3928570104
                }
            }, {
                _index: 'index_051364f0fe4446c2bf95fa4b93e2dbd2_1536899613926',
                _type: 'type',
                _id: 'nDZb1mUBbvDJlUQCLHRu',
                _score: 1,
                _source: {
                    thresh: 75, iso: 'RUS', adm1: 35, adm2: 925, area: 1137359.9711284428
                }
            }, {
                _index: 'index_051364f0fe4446c2bf95fa4b93e2dbd2_1536899613926',
                _type: 'type',
                _id: 'oTZb1mUBbvDJlUQCLHRu',
                _score: 1,
                _source: {
                    thresh: 75, iso: 'COL', adm1: 30, adm2: 1017, area: 128570.48945388374
                }
            }
        ];

        nock(`${process.env.CT_URL}`)
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
                        query: 'SELECT * FROM 051364f0-fe44-46c2-bf95-fa4b93e2dbd2',
                        jsonSql: {
                            select: [
                                {
                                    value: '*',
                                    alias: null,
                                    type: 'wildcard'
                                }
                            ],
                            from: '051364f0-fe44-46c2-bf95-fa4b93e2dbd2'
                        }
                    },
                    relationships: {}
                }
            });


        nock(`http://${elasticUri}`, { encodedQueryParams: true })
            .get('/index_051364f0fe4446c2bf95fa4b93e2dbd2_1536899613926/_mapping')
            .reply(200, {
                index_051364f0fe4446c2bf95fa4b93e2dbd2_1536899613926: {
                    mappings: {
                        type: {
                            properties: {
                                adm1: { type: 'long' },
                                adm2: { type: 'long' },
                                area: { type: 'float' },
                                iso: {
                                    type: 'text',
                                    fields: { keyword: { type: 'keyword', ignore_above: 256 } }
                                },
                                thresh: { type: 'long' }
                            }
                        }
                    }
                }
            }, ['content-type',
                'application/json; charset=UTF-8',
                'content-length',
                '271']);


        nock(`http://${elasticUri}`, { encodedQueryParams: true })
            .post('/_sql/_explain', 'SELECT * FROM index_051364f0fe4446c2bf95fa4b93e2dbd2_1536899613926')
            .reply(200, { from: 0, size: 200 }, ['content-type',
                'text/plain; charset=UTF-8',
                'content-length',
                '21']);


        nock(`http://${elasticUri}`, { encodedQueryParams: true })
            .post('/index_051364f0fe4446c2bf95fa4b93e2dbd2_1536899613926/_search', { from: 0, size: 10000 })
            .query({ scroll: '1m' })
            .reply(200, {
                _scroll_id: 'DnF1ZXJ5VGhlbkZldGNoBQAAAAAAAADJFmFuSG5mU0lEUVIybVhZSVl2V1BkNmcAAAAAAAAAyxZhbkhuZlNJRFFSMm1YWUlZdldQZDZnAAAAAAAAAMoWYW5IbmZTSURRUjJtWFlJWXZXUGQ2ZwAAAAAAAADMFmFuSG5mU0lEUVIybVhZSVl2V1BkNmcAAAAAAAAAzRZhbkhuZlNJRFFSMm1YWUlZdldQZDZn',
                took: 0,
                timed_out: false,
                _shards: {
                    total: 5, successful: 5, skipped: 0, failed: 0
                },
                hits: {
                    total: 16384,
                    max_score: 1,
                    hits: [results[0], results[1]]
                }
            }, ['content-type',
                'application/json; charset=UTF-8',
                'content-length',
                '792']);

        nock(`http://${elasticUri}`, { encodedQueryParams: true })
            .get('/_search/scroll')
            .times(1)
            .query(query => query.scroll === '1m' && query.scroll_id === 'DnF1ZXJ5VGhlbkZldGNoBQAAAAAAAADJFmFuSG5mU0lEUVIybVhZSVl2V1BkNmcAAAAAAAAAyxZhbkhuZlNJRFFSMm1YWUlZdldQZDZnAAAAAAAAAMoWYW5IbmZTSURRUjJtWFlJWXZXUGQ2ZwAAAAAAAADMFmFuSG5mU0lEUVIybVhZSVl2V1BkNmcAAAAAAAAAzRZhbkhuZlNJRFFSMm1YWUlZdldQZDZn')
            .reply(200, {
                _scroll_id: 'DnF1ZXJ5VGhlbkZldGNoBQAAAAAAAADJFmFuSG5mU0lEUVIybVhZSVl2V1BkNmcAAAAAAAAAyxZhbkhuZlNJRFFSMm1YWUlZdldQZDZnAAAAAAAAAMoWYW5IbmZTSURRUjJtWFlJWXZXUGQ2ZwAAAAAAAADMFmFuSG5mU0lEUVIybVhZSVl2V1BkNmcAAAAAAAAAzRZhbkhuZlNJRFFSMm1YWUlZdldQZDZn',
                took: 1,
                timed_out: false,
                terminated_early: true,
                _shards: {
                    total: 5, successful: 5, skipped: 0, failed: 0
                },
                hits: {
                    total: 16384,
                    max_score: 1,
                    hits: [results[2], results[3]]
                }
            }, ['content-type',
                'application/json; charset=UTF-8',
                'content-length',
                '817']);

        nock(`http://${elasticUri}`, { encodedQueryParams: true })
            .get('/_search/scroll')
            .times(1)
            .query(query => query.scroll === '1m' && query.scroll_id === 'DnF1ZXJ5VGhlbkZldGNoBQAAAAAAAADJFmFuSG5mU0lEUVIybVhZSVl2V1BkNmcAAAAAAAAAyxZhbkhuZlNJRFFSMm1YWUlZdldQZDZnAAAAAAAAAMoWYW5IbmZTSURRUjJtWFlJWXZXUGQ2ZwAAAAAAAADMFmFuSG5mU0lEUVIybVhZSVl2V1BkNmcAAAAAAAAAzRZhbkhuZlNJRFFSMm1YWUlZdldQZDZn')
            .reply(200, {
                _scroll_id: 'DnF1ZXJ5VGhlbkZldGNoBQAAAAAAAADJFmFuSG5mU0lEUVIybVhZSVl2V1BkNmcAAAAAAAAAyxZhbkhuZlNJRFFSMm1YWUlZdldQZDZnAAAAAAAAAMoWYW5IbmZTSURRUjJtWFlJWXZXUGQ2ZwAAAAAAAADMFmFuSG5mU0lEUVIybVhZSVl2V1BkNmcAAAAAAAAAzRZhbkhuZlNJRFFSMm1YWUlZdldQZDZn',
                took: 1,
                timed_out: false,
                _shards: {
                    total: 5, successful: 5, skipped: 0, failed: 0
                },
                hits: { total: 16384, max_score: 1, hits: [] }
            }, ['content-type',
                'application/json; charset=UTF-8',
                'content-length',
                '381']);

        const queryResponse = await requester
            .post(`/api/v1/document/query/${requestBody.dataset.id}?sql=${query}`)
            .send(requestBody);

        queryResponse.status.should.equal(200);
        queryResponse.body.should.have.property('data').and.be.an('array');
        queryResponse.body.should.have.property('meta').and.be.an('object');

        queryResponse.body.data.should.have.lengthOf(results.length);

        const resultList = results.map(elem => Object.assign({}, { _id: elem._id }, elem._source));

        queryResponse.body.data.should.deep.equal(resultList);

    });

    it('Group by query to dataset should be successful (happy case)', async () => {
        const requestBody = {
            dataset: {
                name: 'Food Demand',
                slug: 'Food-Demand_3',
                type: null,
                subtitle: null,
                application: [
                    'rw'
                ],
                dataPath: 'data',
                attributesPath: null,
                connectorType: 'document',
                provider: 'json',
                userId: '1a10d7c6e0a37126611fd7a7',
                connectorUrl: 'http://gfw2-data.s3.amazonaws.com/alerts-tsv/output/to-api/output.json',
                tableName: 'index_051364f0fe4446c2bf95fa4b93e2dbd2_1536899613926',
                status: 'saved',
                published: true,
                overwrite: false,
                verified: false,
                blockchain: {},
                mainDateField: null,
                env: 'production',
                geoInfo: false,
                protected: false,
                legend: {
                    date: [],
                    region: [],
                    country: [],
                    nested: []
                },
                clonedHost: {},
                errorMessage: '',
                taskId: '/v1/doc-importer/task/986bd4ee-0bfe-4002-ae17-1d1594dffd0a',
                updatedAt: '2018-09-14T04:33:48.838Z',
                dataLastUpdated: null,
                widgetRelevantProps: [],
                layerRelevantProps: [],
                id: '051364f0-fe44-46c2-bf95-fa4b93e2dbd2'
            },
            loggedUser: null
        };

        const query = `select * from ${requestBody.dataset.id} group by field`;

        const results = [{ key: 'ARM', doc_count: 1 }, { key: 'AUS', doc_count: 1 }, {
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

        nock(`${process.env.CT_URL}`)
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
                        query: 'SELECT * FROM 051364f0-fe44-46c2-bf95-fa4b93e2dbd2',
                        jsonSql: {
                            select: [
                                {
                                    value: '*',
                                    alias: null,
                                    type: 'wildcard'
                                }
                            ],
                            from: '051364f0-fe44-46c2-bf95-fa4b93e2dbd2',
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

        nock(`http://${elasticUri}`, { encodedQueryParams: true })
            .get('/index_051364f0fe4446c2bf95fa4b93e2dbd2_1536899613926/_mapping')
            .reply(200, {
                index_051364f0fe4446c2bf95fa4b93e2dbd2_1536899613926: {
                    mappings: {
                        type: {
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


        nock(`http://${elasticUri}`, { encodedQueryParams: true })
            .get('/index_051364f0fe4446c2bf95fa4b93e2dbd2_1536899613926/_mapping')
            .reply(200, {
                index_051364f0fe4446c2bf95fa4b93e2dbd2_1536899613926: {
                    mappings: {
                        type: {
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


        nock(`http://${elasticUri}`, { encodedQueryParams: true })
            .post('/_sql/_explain', 'SELECT * FROM index_051364f0fe4446c2bf95fa4b93e2dbd2_1536899613926 GROUP BY "ISO.keyword" LIMIT 9999999')
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


        nock(`http://${elasticUri}`, { encodedQueryParams: true })
            .post('/index_051364f0fe4446c2bf95fa4b93e2dbd2_1536899613926/_search', {
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
            })
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
            .post(`/api/v1/document/query/${requestBody.dataset.id}?sql=${query}`)
            .send(requestBody);

        queryResponse.status.should.equal(200);
        queryResponse.body.should.have.property('data').and.be.an('array');
        queryResponse.body.should.have.property('meta').and.be.an('object');

        queryResponse.body.data.should.have.lengthOf(results.length);

        const resultList = results.map(elem => ({ ISO: elem.key }));

        queryResponse.body.data.should.deep.equal(resultList);
    });

    afterEach(() => {
        if (!nock.isDone()) {
            throw new Error(`Not all nock interceptors were used: ${nock.pendingMocks()}`);
        }
    });
});

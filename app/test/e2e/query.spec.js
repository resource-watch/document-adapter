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

describe('Query datasets - Simple queries', () => {

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
                        query: `SELECT * FROM ${requestBody.dataset.id}`,
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

    afterEach(() => {
        if (!nock.isDone()) {
            throw new Error(`Not all nock interceptors were used: ${nock.pendingMocks()}`);
        }
    });
});

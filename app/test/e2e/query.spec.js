/* eslint-disable max-len */
const nock = require('nock');
const config = require('config');
const chai = require('chai');
const { getTestServer } = require('./utils/test-server');
const { createMockGetDataset } = require('./utils/helpers');

chai.should();

const requester = getTestServer();

const elasticUri = process.env.ELASTIC_URI || `${config.get('elasticsearch.host')}:${config.get('elasticsearch.port')}`;

nock.disableNetConnect();
nock.enableNetConnect(`${process.env.HOST_IP}:${process.env.PORT}`);

describe('Query datasets - Simple queries', () => {

    before(async () => {
        if (process.env.NODE_ENV !== 'test') {
            throw Error(`Running the test suite with NODE_ENV ${process.env.NODE_ENV} may result in permanent data loss. Please use NODE_ENV=test.`);
        }
    });

    it('Basic query to dataset should be successful (happy case)', async () => {
        const timestamp = new Date().getTime();

        createMockGetDataset(timestamp);

        const requestBody = {
            loggedUser: null
        };

        const query = `select * from ${timestamp}`;

        const results = [
            {
                _index: 'index_d1ced4227cd5480a8904d3410d75bf42_1587619728489',
                _type: 'type',
                _id: 'kDZb1mUBbvDJlUQCLHRu',
                _score: 1,
                _source: {
                    thresh: 75, iso: 'USA', adm1: 27, adm2: 1641, area: 41420.47960515353
                }
            }, {
                _index: 'index_d1ced4227cd5480a8904d3410d75bf42_1587619728489',
                _type: 'type',
                _id: 'mzZb1mUBbvDJlUQCLHRu',
                _score: 1,
                _source: {
                    thresh: 75, iso: 'BRA', adm1: 12, adm2: 1450, area: 315602.3928570104
                }
            }, {
                _index: 'index_d1ced4227cd5480a8904d3410d75bf42_1587619728489',
                _type: 'type',
                _id: 'nDZb1mUBbvDJlUQCLHRu',
                _score: 1,
                _source: {
                    thresh: 75, iso: 'RUS', adm1: 35, adm2: 925, area: 1137359.9711284428
                }
            }, {
                _index: 'index_d1ced4227cd5480a8904d3410d75bf42_1587619728489',
                _type: 'type',
                _id: 'oTZb1mUBbvDJlUQCLHRu',
                _score: 1,
                _source: {
                    thresh: 75, iso: 'COL', adm1: 30, adm2: 1017, area: 128570.48945388374
                }
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
                        query: `SELECT * FROM ${timestamp}`,
                        jsonSql: {
                            select: [
                                {
                                    value: '*',
                                    alias: null,
                                    type: 'wildcard'
                                }
                            ],
                            from: timestamp
                        }
                    },
                    relationships: {}
                }
            });

        nock(`http://${elasticUri}`, { encodedQueryParams: true })
            .get('/index_d1ced4227cd5480a8904d3410d75bf42_1587619728489/_mapping')
            .reply(200, {
                index_d1ced4227cd5480a8904d3410d75bf42_1587619728489: {
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
            .post('/_sql/_explain', 'SELECT * FROM index_d1ced4227cd5480a8904d3410d75bf42_1587619728489')
            .reply(200, { from: 0, size: 200 }, ['content-type',
                'text/plain; charset=UTF-8',
                'content-length',
                '21']);

        nock(`http://${elasticUri}`, { encodedQueryParams: true })
            .post('/index_d1ced4227cd5480a8904d3410d75bf42_1587619728489/_search', { from: 0, size: 10000 })
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
            .query((ESQuery) => ESQuery.scroll === '1m' && ESQuery.scroll_id === 'DnF1ZXJ5VGhlbkZldGNoBQAAAAAAAADJFmFuSG5mU0lEUVIybVhZSVl2V1BkNmcAAAAAAAAAyxZhbkhuZlNJRFFSMm1YWUlZdldQZDZnAAAAAAAAAMoWYW5IbmZTSURRUjJtWFlJWXZXUGQ2ZwAAAAAAAADMFmFuSG5mU0lEUVIybVhZSVl2V1BkNmcAAAAAAAAAzRZhbkhuZlNJRFFSMm1YWUlZdldQZDZn')
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
            .query((ESQuery) => ESQuery.scroll === '1m' && ESQuery.scroll_id === 'DnF1ZXJ5VGhlbkZldGNoBQAAAAAAAADJFmFuSG5mU0lEUVIybVhZSVl2V1BkNmcAAAAAAAAAyxZhbkhuZlNJRFFSMm1YWUlZdldQZDZnAAAAAAAAAMoWYW5IbmZTSURRUjJtWFlJWXZXUGQ2ZwAAAAAAAADMFmFuSG5mU0lEUVIybVhZSVl2V1BkNmcAAAAAAAAAzRZhbkhuZlNJRFFSMm1YWUlZdldQZDZn')
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
            .post(`/api/v1/document/query/${timestamp}?sql=${encodeURI(query)}`)
            .send(requestBody);

        queryResponse.status.should.equal(200);
        queryResponse.body.should.have.property('data').and.be.an('array');
        queryResponse.body.should.have.property('meta').and.be.an('object');

        queryResponse.body.data.should.have.lengthOf(results.length);

        // eslint-disable-next-line no-underscore-dangle
        const resultList = results.map((elem) => ({ _id: elem._id, ...elem._source }));

        queryResponse.body.data.should.deep.equal(resultList);

    });

    it('Query with special characters to dataset should be successful (happy case)', async () => {
        const timestamp = new Date().getTime();

        createMockGetDataset(timestamp);

        const requestBody = {
            loggedUser: null
        };

        const query = `SELECT * FROM ${timestamp} WHERE foo LIKE '%bar%'`;

        const results = [
            {
                _index: 'index_d1ced4227cd5480a8904d3410d75bf42_1587619728489',
                _type: 'type',
                _id: 'kDZb1mUBbvDJlUQCLHRu',
                _score: 1,
                _source: {
                    thresh: 75, iso: 'USA', adm1: 27, adm2: 1641, area: 41420.47960515353
                }
            }, {
                _index: 'index_d1ced4227cd5480a8904d3410d75bf42_1587619728489',
                _type: 'type',
                _id: 'mzZb1mUBbvDJlUQCLHRu',
                _score: 1,
                _source: {
                    thresh: 75, iso: 'BRA', adm1: 12, adm2: 1450, area: 315602.3928570104
                }
            }, {
                _index: 'index_d1ced4227cd5480a8904d3410d75bf42_1587619728489',
                _type: 'type',
                _id: 'nDZb1mUBbvDJlUQCLHRu',
                _score: 1,
                _source: {
                    thresh: 75, iso: 'RUS', adm1: 35, adm2: 925, area: 1137359.9711284428
                }
            }, {
                _index: 'index_d1ced4227cd5480a8904d3410d75bf42_1587619728489',
                _type: 'type',
                _id: 'oTZb1mUBbvDJlUQCLHRu',
                _score: 1,
                _source: {
                    thresh: 75, iso: 'COL', adm1: 30, adm2: 1017, area: 128570.48945388374
                }
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
                            from: timestamp,
                            where: [
                                {
                                    type: 'operator',
                                    value: 'LIKE',
                                    left: [
                                        {
                                            value: 'foo',
                                            type: 'literal'
                                        }
                                    ],
                                    right: [
                                        {
                                            value: '%bar%',
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

        nock(`http://${elasticUri}`, { encodedQueryParams: true })
            .get('/index_d1ced4227cd5480a8904d3410d75bf42_1587619728489/_mapping')
            .reply(200, {
                index_d1ced4227cd5480a8904d3410d75bf42_1587619728489: {
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
            .post('/_sql/_explain', 'SELECT * FROM index_d1ced4227cd5480a8904d3410d75bf42_1587619728489 WHERE foo LIKE \'%bar%\'')
            .reply(200, { from: 0, size: 200 }, ['content-type',
                'text/plain; charset=UTF-8',
                'content-length',
                '21']);

        nock(`http://${elasticUri}`, { encodedQueryParams: true })
            .post('/index_d1ced4227cd5480a8904d3410d75bf42_1587619728489/_search', { from: 0, size: 10000 })
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
            .query((ESQuery) => ESQuery.scroll === '1m' && ESQuery.scroll_id === 'DnF1ZXJ5VGhlbkZldGNoBQAAAAAAAADJFmFuSG5mU0lEUVIybVhZSVl2V1BkNmcAAAAAAAAAyxZhbkhuZlNJRFFSMm1YWUlZdldQZDZnAAAAAAAAAMoWYW5IbmZTSURRUjJtWFlJWXZXUGQ2ZwAAAAAAAADMFmFuSG5mU0lEUVIybVhZSVl2V1BkNmcAAAAAAAAAzRZhbkhuZlNJRFFSMm1YWUlZdldQZDZn')
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
            .query((ESQuery) => ESQuery.scroll === '1m' && ESQuery.scroll_id === 'DnF1ZXJ5VGhlbkZldGNoBQAAAAAAAADJFmFuSG5mU0lEUVIybVhZSVl2V1BkNmcAAAAAAAAAyxZhbkhuZlNJRFFSMm1YWUlZdldQZDZnAAAAAAAAAMoWYW5IbmZTSURRUjJtWFlJWXZXUGQ2ZwAAAAAAAADMFmFuSG5mU0lEUVIybVhZSVl2V1BkNmcAAAAAAAAAzRZhbkhuZlNJRFFSMm1YWUlZdldQZDZn')
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
            .post(`/api/v1/document/query/${timestamp}?sql=${encodeURI(query)}`)
            .send(requestBody);

        queryResponse.status.should.equal(200);
        queryResponse.body.should.have.property('data').and.be.an('array');
        queryResponse.body.should.have.property('meta').and.be.an('object');

        queryResponse.body.data.should.have.lengthOf(results.length);

        // eslint-disable-next-line no-underscore-dangle
        const resultList = results.map((elem) => ({ _id: elem._id, ...elem._source }));

        queryResponse.body.data.should.deep.equal(resultList);

    });

    it('Query with alias (x AS y) in SELECT clause should be successful (happy case)', async () => {
        const timestamp = new Date().getTime();

        createMockGetDataset(timestamp);

        const requestBody = {
            loggedUser: null
        };

        const query = `SELECT year_data.year AS year FROM ${timestamp}'`;

        const results = [
            {
                _index: 'index_d1ced4227cd5480a8904d3410d75bf42_1587619728489',
                _type: 'type',
                _id: 'AWi9IfXGQ5uNBZJIvxAi',
                _score: 1,
                _source: { year_data: [{ year: 2001 }, { year: 2002 }, { year: 2003 }, { year: 2004 }, { year: 2005 }, { year: 2006 }, { year: 2007 }, { year: 2008 }, { year: 2009 }, { year: 2010 }, { year: 2011 }, { year: 2012 }, { year: 2013 }, { year: 2014 }, { year: 2015 }, { year: 2016 }, { year: 2017 }] }
            }, {
                _index: 'index_d1ced4227cd5480a8904d3410d75bf42_1587619728489',
                _type: 'type',
                _id: 'AWi9IfXGQ5uNBZJIvxAj',
                _score: 1,
                _source: { year_data: [{ year: 2001 }, { year: 2002 }, { year: 2003 }, { year: 2004 }, { year: 2005 }, { year: 2006 }, { year: 2007 }, { year: 2008 }, { year: 2009 }, { year: 2010 }, { year: 2011 }, { year: 2012 }, { year: 2013 }, { year: 2014 }, { year: 2015 }, { year: 2016 }, { year: 2017 }] }
            }, {
                _index: 'index_d1ced4227cd5480a8904d3410d75bf42_1587619728489',
                _type: 'type',
                _id: 'AWi9IfXGQ5uNBZJIvxAl',
                _score: 1,
                _source: { year_data: [{ year: 2001 }, { year: 2002 }, { year: 2003 }, { year: 2004 }, { year: 2005 }, { year: 2006 }, { year: 2007 }, { year: 2008 }, { year: 2009 }, { year: 2010 }, { year: 2011 }, { year: 2012 }, { year: 2013 }, { year: 2014 }, { year: 2015 }, { year: 2016 }, { year: 2017 }] }
            }, {
                _index: 'index_d1ced4227cd5480a8904d3410d75bf42_1587619728489',
                _type: 'type',
                _id: 'AWi9IfXGQ5uNBZJIvxAq',
                _score: 1,
                _source: { year_data: [{ year: 2001 }, { year: 2002 }, { year: 2003 }, { year: 2004 }, { year: 2005 }, { year: 2006 }, { year: 2007 }, { year: 2008 }, { year: 2009 }, { year: 2010 }, { year: 2011 }, { year: 2012 }, { year: 2013 }, { year: 2014 }, { year: 2015 }, { year: 2016 }, { year: 2017 }] }
            }
        ];

        nock(process.env.CT_URL)
            .get('/v1/convert/sql2SQL')
            .query({ sql: query })
            .reply(200, {
                data: {
                    type: 'result',
                    id: 'undefined',
                    attributes: {
                        query: 'SELECT year_data.year AS year FROM index_d1ced4227cd5480a8904d3410d75bf42_1587619728489',
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
                            from: 'index_d1ced4227cd5480a8904d3410d75bf42_1587619728489'
                        }
                    },
                    relationships: {}
                }
            });

        nock(`http://${elasticUri}`)
            .get('/index_d1ced4227cd5480a8904d3410d75bf42_1587619728489/_mapping')
            .reply(200, {
                index_d1ced4227cd5480a8904d3410d75bf42_1587619728489: {
                    mappings: {
                        type: {
                            properties: {
                                adm1: { type: 'long' },
                                adm2: { type: 'long' },
                                area_admin: { type: 'float' },
                                area_extent: { type: 'float' },
                                area_extent_2000: { type: 'float' },
                                area_gain: { type: 'float' },
                                area_poly_aoi: { type: 'float' },
                                bound1: {
                                    type: 'text',
                                    fields: { keyword: { type: 'keyword', ignore_above: 256 } }
                                },
                                bound2: {
                                    type: 'text',
                                    fields: { keyword: { type: 'keyword', ignore_above: 256 } }
                                },
                                bound3: { type: 'long' },
                                bound4: { type: 'long' },
                                iso: {
                                    type: 'text',
                                    fields: { keyword: { type: 'keyword', ignore_above: 256 } }
                                },
                                polyname: {
                                    type: 'text',
                                    fields: { keyword: { type: 'keyword', ignore_above: 256 } }
                                },
                                thresh: { type: 'long' },
                                year_data: {
                                    properties: {
                                        area_loss: { type: 'long' },
                                        emissions: { type: 'long' },
                                        year: { type: 'long' }
                                    }
                                }
                            }
                        }
                    }
                }
            });

        nock(`http://${elasticUri}`)
            .post('/_sql/_explain', 'SELECT year_data.year FROM index_d1ced4227cd5480a8904d3410d75bf42_1587619728489')
            .reply(200, {
                from: 0,
                size: 200,
                _source: { includes: ['year_data.year'], excludes: [] }
            }, ['content-type',
                'text/plain; charset=UTF-8']);

        nock(`http://${elasticUri}`)
            .post('/index_d1ced4227cd5480a8904d3410d75bf42_1587619728489/_search', {
                from: 0,
                size: 10000,
                _source: { includes: ['year_data.year'], excludes: [] }
            })
            .query({ scroll: '1m' })
            .reply(200, {
                _scroll_id: 'DnF1ZXJ5VGhlbkZldGNoBQAAAAAAAABRFjJSVFh4SXR0U0E2OF81dmJSTzIyd1EAAAAAAAAAUhYyUlRYeEl0dFNBNjhfNXZiUk8yMndRAAAAAAAAAFMWMlJUWHhJdHRTQTY4XzV2YlJPMjJ3UQAAAAAAAABUFjJSVFh4SXR0U0E2OF81dmJSTzIyd1EAAAAAAAAAVRYyUlRYeEl0dFNBNjhfNXZiUk8yMndR',
                took: 2,
                timed_out: false,
                _shards: { total: 5, successful: 5, failed: 0 },
                hits: {
                    total: 99,
                    max_score: 1,
                    hits: [results[0], results[1]]
                }
            });

        nock(`http://${elasticUri}`)
            .get('/_search/scroll')
            .times(1)
            .query((ESQuery) => ESQuery.scroll === '1m' && ESQuery.scroll_id === 'DnF1ZXJ5VGhlbkZldGNoBQAAAAAAAABRFjJSVFh4SXR0U0E2OF81dmJSTzIyd1EAAAAAAAAAUhYyUlRYeEl0dFNBNjhfNXZiUk8yMndRAAAAAAAAAFMWMlJUWHhJdHRTQTY4XzV2YlJPMjJ3UQAAAAAAAABUFjJSVFh4SXR0U0E2OF81dmJSTzIyd1EAAAAAAAAAVRYyUlRYeEl0dFNBNjhfNXZiUk8yMndR')
            .reply(200, {
                _scroll_id: 'DnF1ZXJ5VGhlbkZldGNoBQAAAAAAAABRFjJSVFh4SXR0U0E2OF81dmJSTzIyd1EAAAAAAAAAUhYyUlRYeEl0dFNBNjhfNXZiUk8yMndRAAAAAAAAAFMWMlJUWHhJdHRTQTY4XzV2YlJPMjJ3UQAAAAAAAABUFjJSVFh4SXR0U0E2OF81dmJSTzIyd1EAAAAAAAAAVRYyUlRYeEl0dFNBNjhfNXZiUk8yMndR',
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
            });

        nock(`http://${elasticUri}`)
            .get('/_search/scroll')
            .times(1)
            .query((ESQuery) => ESQuery.scroll === '1m' && ESQuery.scroll_id === 'DnF1ZXJ5VGhlbkZldGNoBQAAAAAAAABRFjJSVFh4SXR0U0E2OF81dmJSTzIyd1EAAAAAAAAAUhYyUlRYeEl0dFNBNjhfNXZiUk8yMndRAAAAAAAAAFMWMlJUWHhJdHRTQTY4XzV2YlJPMjJ3UQAAAAAAAABUFjJSVFh4SXR0U0E2OF81dmJSTzIyd1EAAAAAAAAAVRYyUlRYeEl0dFNBNjhfNXZiUk8yMndR')
            .reply(200, {
                _scroll_id: 'DnF1ZXJ5VGhlbkZldGNoBQAAAAAAAABRFjJSVFh4SXR0U0E2OF81dmJSTzIyd1EAAAAAAAAAUhYyUlRYeEl0dFNBNjhfNXZiUk8yMndRAAAAAAAAAFMWMlJUWHhJdHRTQTY4XzV2YlJPMjJ3UQAAAAAAAABUFjJSVFh4SXR0U0E2OF81dmJSTzIyd1EAAAAAAAAAVRYyUlRYeEl0dFNBNjhfNXZiUk8yMndR',
                took: 1,
                timed_out: false,
                _shards: {
                    total: 5, successful: 5, skipped: 0, failed: 0
                },
                hits: { total: 16384, max_score: 1, hits: [] }
            });

        const queryResponse = await requester
            .post(`/api/v1/document/query/${timestamp}?sql=${encodeURI(query)}`)
            .send(requestBody);

        queryResponse.status.should.equal(200);
        queryResponse.body.should.have.property('data').and.be.an('array');
        queryResponse.body.should.have.property('meta').and.be.an('object');

        queryResponse.body.data.should.have.lengthOf(results.length);

        // eslint-disable-next-line no-underscore-dangle
        const resultList = results.map((elem) => ({ _id: elem._id, ...elem._source }));

        queryResponse.body.data.should.deep.equal(resultList);

    });

    it('Query with order by mapping from Elasticsearch should be successful (happy case)', async () => {
        const timestamp = new Date().getTime();

        createMockGetDataset(timestamp);

        const requestBody = {
            loggedUser: null
        };

        const query = `select numeric_m as x, A1B_2080s as y, month as z from ${timestamp} where streamflow='Snohomish' order by numeric_m ASC'`;

        const results = [
            {
                _index: 'index_d1ced4227cd5480a8904d3410d75bf42_1587619728489',
                _type: 'type',
                _id: 'AXDtQNBj5xxOSI2DEp2n',
                _score: null,
                _source: {
                    numeric_m: 1,
                    month: 'Oct',
                    A1B_2080s: 6933.430769
                },
                sort: [
                    1
                ]
            },
            {
                _index: 'index_d1ced4227cd5480a8904d3410d75bf42_1587619728489',
                _type: 'type',
                _id: 'AXDtQNBj5xxOSI2DEp2o',
                _score: null,
                _source: {
                    numeric_m: 2,
                    month: 'Nov',
                    A1B_2080s: 15743.70549
                },
                sort: [
                    2
                ]
            },
            {
                _index: 'index_d1ced4227cd5480a8904d3410d75bf42_1587619728489',
                _type: 'type',
                _id: 'AXDtQNBj5xxOSI2DEp2p',
                _score: null,
                _source: {
                    numeric_m: 3,
                    month: 'Dec',
                    A1B_2080s: 20168.46484
                },
                sort: [
                    3
                ]
            },
            {
                _index: 'index_d1ced4227cd5480a8904d3410d75bf42_1587619728489',
                _type: 'type',
                _id: 'AXDtQNBj5xxOSI2DEp2q',
                _score: null,
                _source: {
                    numeric_m: 4,
                    month: 'Jan',
                    A1B_2080s: 19139.62198
                },
                sort: [
                    4
                ]
            }
        ];

        nock(process.env.CT_URL)
            .get('/v1/convert/sql2SQL')
            .query({ sql: query })
            .reply(200, {
                data: {
                    type: 'result',
                    attributes: {
                        query: 'SELECT numeric_m AS x, A1B_2080s AS y, month AS z FROM index_d1ced4227cd5480a8904d3410d75bf42_1587619728489 WHERE streamflow = \'Snohomish\' ORDER BY numeric_m ASC',
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
                            from: 'index_d1ced4227cd5480a8904d3410d75bf42_1587619728489',
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

        nock(`http://${elasticUri}`)
            .get('/index_d1ced4227cd5480a8904d3410d75bf42_1587619728489/_mapping')
            .twice()
            .reply(200, {
                index_d1ced4227cd5480a8904d3410d75bf42_1587619728489: {
                    mappings: {
                        index_d1ced4227cd5480a8904d3410d75bf42_1587619728489: {
                            properties: {
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
                                    type: 'string'
                                },
                                numeric_m: {
                                    type: 'long'
                                },
                                streamflow: {
                                    type: 'string'
                                }
                            }
                        }
                    }
                }
            });

        nock(`http://${elasticUri}`)
            .post('/_sql/_explain', 'SELECT numeric_m, A1B_2080s, month FROM index_d1ced4227cd5480a8904d3410d75bf42_1587619728489 WHERE streamflow = \'Snohomish\' ORDER BY numeric_m ASC LIMIT 9999999')
            .reply(200, {
                from: 0,
                size: 9999999,
                query: {
                    bool: {
                        filter: [
                            {
                                bool: {
                                    must: [
                                        {
                                            match_phrase: {
                                                streamflow: {
                                                    query: 'Snohomish',
                                                    slop: 0,
                                                    boost: 1.0
                                                }
                                            }
                                        }
                                    ],
                                    disable_coord: false,
                                    adjust_pure_negative: true,
                                    boost: 1.0
                                }
                            }
                        ],
                        disable_coord: false,
                        adjust_pure_negative: true,
                        boost: 1.0
                    }
                },
                _source: {
                    includes: [
                        'numeric_m',
                        'A1B_2080s',
                        'month'
                    ],
                    excludes: []
                },
                sort: [
                    {
                        numeric_m: {
                            order: 'asc'
                        }
                    }
                ]
            }, ['content-type',
                'text/plain; charset=UTF-8']);

        nock(`http://${elasticUri}`)
            .post('/index_d1ced4227cd5480a8904d3410d75bf42_1587619728489/_search',
                {
                    from: 0,
                    size: 10000,
                    query: {
                        bool: {
                            filter: [{
                                bool: {
                                    must: [{
                                        match_phrase: {
                                            streamflow: {
                                                query: 'Snohomish',
                                                slop: 0,
                                                boost: 1
                                            }
                                        }
                                    }],
                                    disable_coord: false,
                                    adjust_pure_negative: true,
                                    boost: 1
                                }
                            }],
                            disable_coord: false,
                            adjust_pure_negative: true,
                            boost: 1
                        }
                    },
                    _source: { includes: ['numeric_m', 'A1B_2080s', 'month'], excludes: [] },
                    sort: [{ numeric_m: { order: 'asc' } }]
                })
            .query({ scroll: '1m' })
            .reply(200, {
                _scroll_id: 'DnF1ZXJ5VGhlbkZldGNoAwAAAAAAAAAbFlEyNDFqczN0UzFpcVlxSHdaZC1QN2cAAAAAAAAAGRZRMjQxanMzdFMxaXFZcUh3WmQtUDdnAAAAAAAAABoWUTI0MWpzM3RTMWlxWXFId1pkLVA3Zw==',
                took: 1,
                timed_out: false,
                _shards: {
                    total: 3,
                    successful: 3,
                    failed: 0
                },
                hits: {
                    total: 4,
                    max_score: null,
                    hits: [results[0], results[1]]
                }
            });

        nock(`http://${elasticUri}`)
            .get('/_search/scroll')
            .times(1)
            .query((ESQuery) => ESQuery.scroll === '1m' && ESQuery.scroll_id === 'DnF1ZXJ5VGhlbkZldGNoAwAAAAAAAAAbFlEyNDFqczN0UzFpcVlxSHdaZC1QN2cAAAAAAAAAGRZRMjQxanMzdFMxaXFZcUh3WmQtUDdnAAAAAAAAABoWUTI0MWpzM3RTMWlxWXFId1pkLVA3Zw==')
            .reply(200, {
                _scroll_id: 'DnF1ZXJ5VGhlbkZldGNoAwAAAAAAAAAbFlEyNDFqczN0UzFpcVlxSHdaZC1QN2cAAAAAAAAAGRZRMjQxanMzdFMxaXFZcUh3WmQtUDdnAAAAAAAAABoWUTI0MWpzM3RTMWlxWXFId1pkLVA3Zw==',
                took: 1,
                timed_out: false,
                terminated_early: true,
                _shards: {
                    total: 5, successful: 5, skipped: 0, failed: 0
                },
                hits: {
                    total: 4,
                    max_score: 1,
                    hits: [results[2], results[3]]
                }
            });

        nock(`http://${elasticUri}`)
            .get('/_search/scroll')
            .times(1)
            .query((ESQuery) => ESQuery.scroll === '1m' && ESQuery.scroll_id === 'DnF1ZXJ5VGhlbkZldGNoAwAAAAAAAAAbFlEyNDFqczN0UzFpcVlxSHdaZC1QN2cAAAAAAAAAGRZRMjQxanMzdFMxaXFZcUh3WmQtUDdnAAAAAAAAABoWUTI0MWpzM3RTMWlxWXFId1pkLVA3Zw==')
            .reply(200, {
                _scroll_id: 'DnF1ZXJ5VGhlbkZldGNoAwAAAAAAAAAbFlEyNDFqczN0UzFpcVlxSHdaZC1QN2cAAAAAAAAAGRZRMjQxanMzdFMxaXFZcUh3WmQtUDdnAAAAAAAAABoWUTI0MWpzM3RTMWlxWXFId1pkLVA3Zw==',
                took: 1,
                timed_out: false,
                _shards: {
                    total: 5, successful: 5, skipped: 0, failed: 0
                },
                hits: { total: 4, max_score: 1, hits: [] }
            });

        const queryResponse = await requester
            .post(`/api/v1/document/query/${timestamp}?sql=${encodeURI(query)}`)
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
                _id: 'AXDtQNBj5xxOSI2DEp2n',
                x: 1,
                y: 6933.430769,
                z: 'Oct'
            },
            {
                numeric_m: 2,
                month: 'Nov',
                A1B_2080s: 15743.70549,
                _id: 'AXDtQNBj5xxOSI2DEp2o',
                x: 2,
                y: 15743.70549,
                z: 'Nov'
            },
            {
                numeric_m: 3,
                month: 'Dec',
                A1B_2080s: 20168.46484,
                _id: 'AXDtQNBj5xxOSI2DEp2p',
                x: 3,
                y: 20168.46484,
                z: 'Dec'
            },
            {
                numeric_m: 4,
                month: 'Jan',
                A1B_2080s: 19139.62198,
                _id: 'AXDtQNBj5xxOSI2DEp2q',
                x: 4,
                y: 19139.62198,
                z: 'Jan'
            }
        ];

        queryResponse.body.data.should.deep.equal(resultList);

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

/* eslint-disable max-len */
const nock = require('nock');
const chai = require('chai');
const config = require('config');
const { getTestServer } = require('./utils/test-server');
const { createMockGetDataset } = require('./utils/helpers');

chai.should();

const requester = getTestServer();

describe('Dataset download tests', () => {

    before(async () => {
        if (process.env.NODE_ENV !== 'test') {
            throw Error(`Running the test suite with NODE_ENV ${process.env.NODE_ENV} may result in permanent data loss. Please use NODE_ENV=test.`);
        }
    });

    it('Download with CSV format and a query that returns no results should be successful', async () => {
        const timestamp = new Date().getTime();

        createMockGetDataset(timestamp);

        const elasticUri = config.get('elasticsearch.host');

        const query = 'SELECT treecover_loss__year, SUM(aboveground_biomass_loss__Mg) AS aboveground_biomass_loss__Mg, SUM(aboveground_co2_emissions__Mg) AS aboveground_co2_emissions__Mg, SUM(treecover_loss__ha) AS treecover_loss__ha FROM data WHERE geostore__id = \'f84d74e5dc977606da07cebaf94dc9e6\' AND treecover_density__threshold = 30 GROUP BY treecover_loss__year ORDER BY treecover_loss__year';

        nock(process.env.CT_URL)
            .get('/v1/convert/sql2SQL')
            .query({ sql: query })
            .reply(200, {
                data: {
                    type: 'result',
                    attributes: {
                        query: 'SELECT treecover_loss__year, SUM(aboveground_biomass_loss__Mg) AS aboveground_biomass_loss__Mg, SUM(aboveground_co2_emissions__Mg) AS aboveground_co2_emissions__Mg, SUM(treecover_loss__ha) AS treecover_loss__ha FROM data WHERE geostore__id = \'f84d74e5dc977606da07cebaf94dc9e6\' AND treecover_density__threshold = 30 GROUP BY treecover_loss__year ORDER BY treecover_loss__year',
                        jsonSql: {
                            select: [{
                                value: 'treecover_loss__year',
                                alias: null,
                                type: 'literal'
                            }, {
                                type: 'function',
                                alias: 'aboveground_biomass_loss__Mg',
                                value: 'SUM',
                                arguments: [{ value: 'aboveground_biomass_loss__Mg', type: 'literal' }]
                            }, {
                                type: 'function',
                                alias: 'aboveground_co2_emissions__Mg',
                                value: 'SUM',
                                arguments: [{ value: 'aboveground_co2_emissions__Mg', type: 'literal' }]
                            }, {
                                type: 'function',
                                alias: 'treecover_loss__ha',
                                value: 'SUM',
                                arguments: [{ value: 'treecover_loss__ha', type: 'literal' }]
                            }],
                            from: 'data',
                            where: [{
                                type: 'conditional',
                                value: 'AND',
                                left: [{
                                    type: 'operator',
                                    value: '=',
                                    left: [{ value: 'geostore__id', type: 'literal' }],
                                    right: [{ value: 'f84d74e5dc977606da07cebaf94dc9e6', type: 'string' }]
                                }],
                                right: [{
                                    type: 'operator',
                                    value: '=',
                                    left: [{ value: 'treecover_density__threshold', type: 'literal' }],
                                    right: [{ value: 30, type: 'number' }]
                                }]
                            }],
                            group: [{ type: 'literal', value: 'treecover_loss__year' }],
                            orderBy: [{
                                type: 'literal',
                                value: 'treecover_loss__year',
                                alias: null,
                                direction: null
                            }]
                        }
                    }
                }
            });

        nock(elasticUri)
            .get('/index_d1ced4227cd5480a8904d3410d75bf42_1587619728489/_mapping')
            .reply(200, {
                index_d1ced4227cd5480a8904d3410d75bf42_1587619728489: {
                    mappings: {
                        properties: {
                            aboveground_biomass_loss__Mg: { type: 'float' },
                            aboveground_co2_emissions__Mg: { type: 'float' },
                            geostore__id: {
                                type: 'text',
                                fields: { keyword: { type: 'keyword', ignore_above: 256 } }
                            },
                            gfw_plantation__type: { type: 'long' },
                            global_land_cover__class: {
                                type: 'text',
                                fields: { keyword: { type: 'keyword', ignore_above: 256 } }
                            },
                            intact_forest_landscape__year: { type: 'long' },
                            is__alliance_for_zero_extinction_site: { type: 'boolean' },
                            is__gfw_land_right: { type: 'boolean' },
                            is__gfw_logging: { type: 'boolean' },
                            is__gfw_mining: { type: 'boolean' },
                            is__gfw_oil_palm: { type: 'boolean' },
                            is__gfw_resource_right: { type: 'boolean' },
                            is__gfw_wood_fiber: { type: 'boolean' },
                            is__idn_forest_moratorium: { type: 'boolean' },
                            is__key_biodiversity_area: { type: 'boolean' },
                            is__landmark: { type: 'boolean' },
                            is__mangroves_1996: { type: 'boolean' },
                            is__mangroves_2016: { type: 'boolean' },
                            is__peat_land: { type: 'boolean' },
                            is__regional_primary_forest: { type: 'boolean' },
                            is__tiger_conservation_landscape: { type: 'boolean' },
                            tcs_driver__type: {
                                type: 'text',
                                fields: { keyword: { type: 'keyword', ignore_above: 256 } }
                            },
                            treecover_density__threshold: { type: 'long' },
                            treecover_loss__ha: { type: 'float' },
                            treecover_loss__year: { type: 'long' },
                            wdpa_protected_area__iucn_cat: {
                                type: 'text',
                                fields: { keyword: { type: 'keyword', ignore_above: 256 } }
                            }
                        }
                    }
                }
            });

        nock(elasticUri)
            .get('/index_d1ced4227cd5480a8904d3410d75bf42_1587619728489/_mapping')
            .reply(200, {
                index_d1ced4227cd5480a8904d3410d75bf42_1587619728489: {
                    mappings: {
                        properties: {
                            aboveground_biomass_loss__Mg: { type: 'float' },
                            aboveground_co2_emissions__Mg: { type: 'float' },
                            geostore__id: {
                                type: 'text',
                                fields: { keyword: { type: 'keyword', ignore_above: 256 } }
                            },
                            gfw_plantation__type: { type: 'long' },
                            global_land_cover__class: {
                                type: 'text',
                                fields: { keyword: { type: 'keyword', ignore_above: 256 } }
                            },
                            intact_forest_landscape__year: { type: 'long' },
                            is__alliance_for_zero_extinction_site: { type: 'boolean' },
                            is__gfw_land_right: { type: 'boolean' },
                            is__gfw_logging: { type: 'boolean' },
                            is__gfw_mining: { type: 'boolean' },
                            is__gfw_oil_palm: { type: 'boolean' },
                            is__gfw_resource_right: { type: 'boolean' },
                            is__gfw_wood_fiber: { type: 'boolean' },
                            is__idn_forest_moratorium: { type: 'boolean' },
                            is__key_biodiversity_area: { type: 'boolean' },
                            is__landmark: { type: 'boolean' },
                            is__mangroves_1996: { type: 'boolean' },
                            is__mangroves_2016: { type: 'boolean' },
                            is__peat_land: { type: 'boolean' },
                            is__regional_primary_forest: { type: 'boolean' },
                            is__tiger_conservation_landscape: { type: 'boolean' },
                            tcs_driver__type: {
                                type: 'text',
                                fields: { keyword: { type: 'keyword', ignore_above: 256 } }
                            },
                            treecover_density__threshold: { type: 'long' },
                            treecover_loss__ha: { type: 'float' },
                            treecover_loss__year: { type: 'long' },
                            wdpa_protected_area__iucn_cat: {
                                type: 'text',
                                fields: { keyword: { type: 'keyword', ignore_above: 256 } }
                            }
                        }
                    }
                }
            });

        nock(elasticUri)
            .post('/_opendistro/_sql/_explain')
            .reply(200, {
                query: {
                    bool: {
                        filter: [
                            {
                                bool: {
                                    must: [
                                        {
                                            bool: {
                                                must: [
                                                    {
                                                        match_phrase: {
                                                            geostore__id: {
                                                                query: 'f84d74e5dc977606da07cebaf94dc9e6',
                                                                slop: 0,
                                                                boost: 1
                                                            }
                                                        }
                                                    },
                                                    {
                                                        match_phrase: {
                                                            treecover_density__threshold: {
                                                                query: 30,
                                                                slop: 0,
                                                                boost: 1
                                                            }
                                                        }
                                                    }
                                                ],
                                                disable_coord: false,
                                                adjust_pure_negative: true,
                                                boost: 1
                                            }
                                        }
                                    ],
                                    disable_coord: false,
                                    adjust_pure_negative: true,
                                    boost: 1
                                }
                            }
                        ],
                        disable_coord: false,
                        adjust_pure_negative: true,
                        boost: 1
                    }
                },
                _source: {
                    includes: [
                        'treecover_loss__year',
                        'SUM',
                        'SUM',
                        'SUM'
                    ],
                    excludes: []
                },
                stored_fields: 'treecover_loss__year',
                sort: [
                    {
                        treecover_loss__year: {
                            order: 'asc'
                        }
                    }
                ],
                aggregations: {
                    treecover_loss__year: {
                        terms: {
                            field: 'treecover_loss__year',
                            size: 9999999,
                            min_doc_count: 1,
                            shard_min_doc_count: 0,
                            show_term_doc_count_error: false,
                            order: {
                                _term: 'asc'
                            }
                        },
                        aggregations: {
                            aboveground_biomass_loss__Mg: {
                                sum: {
                                    field: 'aboveground_biomass_loss__Mg'
                                }
                            },
                            aboveground_co2_emissions__Mg: {
                                sum: {
                                    field: 'aboveground_co2_emissions__Mg'
                                }
                            },
                            treecover_loss__ha: {
                                sum: {
                                    field: 'treecover_loss__ha'
                                }
                            }
                        }
                    }
                }
            }, ['content-type', 'text/plain; charset=UTF-8']);

        nock(elasticUri)
            .post('/index_d1ced4227cd5480a8904d3410d75bf42_1587619728489/_search', {
                query: {
                    bool: {
                        filter: [{
                            bool: {
                                must: [{
                                    bool: {
                                        must: [{
                                            match_phrase: {
                                                geostore__id: {
                                                    query: 'f84d74e5dc977606da07cebaf94dc9e6',
                                                    slop: 0,
                                                    boost: 1
                                                }
                                            }
                                        }, {
                                            match_phrase: {
                                                treecover_density__threshold: {
                                                    query: 30,
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
                        }],
                        disable_coord: false,
                        adjust_pure_negative: true,
                        boost: 1
                    }
                },
                _source: { includes: ['treecover_loss__year', 'SUM', 'SUM', 'SUM'], excludes: [] },
                stored_fields: 'treecover_loss__year',
                sort: [{ treecover_loss__year: { order: 'asc' } }],
                aggregations: {
                    treecover_loss__year: {
                        terms: {
                            field: 'treecover_loss__year',
                            size: 9999999,
                            min_doc_count: 1,
                            shard_min_doc_count: 0,
                            show_term_doc_count_error: false,
                            order: { _term: 'asc' }
                        },
                        aggregations: {
                            aboveground_biomass_loss__Mg: { sum: { field: 'aboveground_biomass_loss__Mg' } },
                            aboveground_co2_emissions__Mg: { sum: { field: 'aboveground_co2_emissions__Mg' } },
                            treecover_loss__ha: { sum: { field: 'treecover_loss__ha' } }
                        }
                    }
                }
            })
            .query({ scroll: '1m' })
            .reply(200, {
                _scroll_id: 'DnF1ZXJ5VGhlbkZldGNoAwAAAAAAAAB_FlEyNDFqczN0UzFpcVlxSHdaZC1QN2cAAAAAAAAAgBZRMjQxanMzdFMxaXFZcUh3WmQtUDdnAAAAAAAAAIEWUTI0MWpzM3RTMWlxWXFId1pkLVA3Zw==',
                took: 1,
                timed_out: false,
                _shards: { total: 3, successful: 3, failed: 0 },
                hits: { total: 0, max_score: 0, hits: [] },
                aggregations: {
                    treecover_loss__year: {
                        doc_count_error_upper_bound: 0,
                        sum_other_doc_count: 0,
                        buckets: []
                    }
                }
            });

        const response = await requester
            .post(`/api/v1/document/download/${timestamp}`)
            .query({ sql: query })
            .send();

        response.status.should.equal(200);
        response.body.should.equal('');
    });

    it('Download with invalid format should return a 400', async () => {
        const timestamp = new Date().getTime();

        const response = await requester
            .post(`/api/v1/document/download/${timestamp}`)
            .query({ sql: '', format: 'potato' })
            .send();

        response.status.should.equal(400);
        response.body.should.have.property('errors').and.be.an('array');
        response.body.errors[0].should.have.property('detail').and.equal(`- format: format must be in [json,csv]. - `);
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

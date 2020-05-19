const nock = require('nock');
const chai = require('chai');
const config = require('config');
const { getTestServer } = require('./test-server');

chai.should();

const requester = getTestServer();

describe('Dataset download tests', () => {

    before(async () => {
        if (process.env.NODE_ENV !== 'test') {
            throw Error(`Running the test suite with NODE_ENV ${process.env.NODE_ENV} may result in permanent data loss. Please use NODE_ENV=test.`);
        }
    });

    it('Download with CSV format and a query that returns no results should be sucessfull', async () => {
        const elasticUri = process.env.ELASTIC_URI || `${config.get('elasticsearch.host')}:${config.get('elasticsearch.port')}`;

        const query = 'SELECT treecover_loss__year, SUM(aboveground_biomass_loss__Mg) AS aboveground_biomass_loss__Mg, SUM(aboveground_co2_emissions__Mg) AS aboveground_co2_emissions__Mg, SUM(treecover_loss__ha) AS treecover_loss__ha FROM data WHERE geostore__id = \'f84d74e5dc977606da07cebaf94dc9e6\' AND treecover_density__threshold = 30 GROUP BY treecover_loss__year ORDER BY treecover_loss__year';

        nock(process.env.CT_URL)
            .get('/v1/convert/sql2SQL')
            .once()
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


        nock(`http://${elasticUri}`)
            .get('/index_d1ced4227cd5480a8904d3410d75bf42_1587619728489/_mapping')
            .reply(200, {
                index_d1ced4227cd5480a8904d3410d75bf42_1587619728489: {
                    mappings: {
                        type: {
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
                }
            });

        nock(`http://${elasticUri}`)
            .get('/index_d1ced4227cd5480a8904d3410d75bf42_1587619728489/_mapping')
            .reply(200, {
                index_d1ced4227cd5480a8904d3410d75bf42_1587619728489: {
                    mappings: {
                        type: {
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
                }
            });

        nock(`http://${elasticUri}`)
            .post('/_sql/_explain', 'SELECT treecover_loss__year, SUM(aboveground_biomass_loss__Mg) AS aboveground_biomass_loss__Mg, SUM(aboveground_co2_emissions__Mg) AS aboveground_co2_emissions__Mg, SUM(treecover_loss__ha) AS treecover_loss__ha FROM index_d1ced4227cd5480a8904d3410d75bf42_1587619728489 WHERE geostore__id = \'f84d74e5dc977606da07cebaf94dc9e6\' AND treecover_density__threshold = 30 GROUP BY treecover_loss__year ORDER BY treecover_loss__year LIMIT 9999999')
            .reply(200, {
                from: 0,
                size: 0,
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


        nock(`http://${elasticUri}`)
            .post('/index_d1ced4227cd5480a8904d3410d75bf42_1587619728489/_search', {
                from: 0,
                size: 0,
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
            .post(`/api/v1/document/download/d1ced422-7cd5-480a-8904-d3410d75bf42`)
            .query({ sql: query })
            .send({
                dataset: {
                    name: 'Tree Cover Loss 2018 Change - Geostore - v20191213',
                    slug: 'Tree-Cover-Loss-2018-Change-Geostore-v20191213_1',
                    type: null,
                    subtitle: null,
                    application: [
                        'gfw'
                    ],
                    dataPath: null,
                    attributesPath: null,
                    connectorType: 'document',
                    provider: 'tsv',
                    userId: '5c65c4b9529ae7001113fd06',
                    connectorUrl: null,
                    sources: [
                        'https://gfw-pipelines.s3.amazonaws.com/geotrellis/results/new_user_aoi/2020-03-28/annualupdate_minimal_20200328_1837/geostore/change/part-00000-ea17fbc5-7d5e-49b0-8fb3-2da90aeccb5a-c000.csv'
                    ],
                    tableName: 'index_d1ced4227cd5480a8904d3410d75bf42_1587619728489',
                    status: 'saved',
                    published: true,
                    overwrite: true,
                    mainDateField: null,
                    env: 'production',
                    geoInfo: false,
                    protected: false,
                    legend: {
                        date: [],
                        region: [],
                        country: [],
                        nested: [],
                        integer: [],
                        short: [],
                        byte: [],
                        double: [],
                        float: [],
                        halfFloat: [],
                        scaledFloat: [],
                        boolean: [],
                        binary: [],
                        text: [],
                        keyword: []
                    },
                    clonedHost: {},
                    errorMessage: 'Url not found: https://gfw-pipelines.s3.amazonaws.com/geotrellis/results/new_user_aoi/2020-04-02/annualupdate_minimal_20200402_0437/geostore/change/part-00009-90dabc00-d956-4cde-bbdc-9f27b0812041-c000.csv',
                    taskId: '/v1/doc-importer/task/4445e303-5d73-440f-9400-da3c466d1878',
                    createdAt: '2020-02-10T21:04:49.635Z',
                    updatedAt: '2020-04-23T09:53:07.019Z',
                    dataLastUpdated: null,
                    widgetRelevantProps: [],
                    layerRelevantProps: [],
                    id: 'd1ced422-7cd5-480a-8904-d3410d75bf42'
                },
            });

        response.status.should.equal(200);
        response.body.should.equal('');
    });

    it('Download with invalid format should return a 400', async () => {
        const response = await requester
            .post(`/api/v1/document/download/d1ced422-7cd5-480a-8904-d3410d75bf42`)
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

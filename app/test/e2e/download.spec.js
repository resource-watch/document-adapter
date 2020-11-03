/* eslint-disable max-len */
const nock = require('nock');
const chai = require('chai');
const { getTestServer } = require('./utils/test-server');
const {
    createMockGetDataset, createIndex, deleteTestIndeces, hasOpenScrolls
} = require('./utils/helpers');

chai.should();

const requester = getTestServer();

nock.disableNetConnect();
nock.enableNetConnect((host) => [`${process.env.HOST_IP}:${process.env.PORT}`, process.env.ELASTIC_TEST_URL].includes(host));

describe('Dataset download tests', () => {

    before(async () => {
        if (process.env.NODE_ENV !== 'test') {
            throw Error(`Running the test suite with NODE_ENV ${process.env.NODE_ENV} may result in permanent data loss. Please use NODE_ENV=test.`);
        }
    });

    it('Download from a dataset with an incorrect provider should fail', async () => {
        const datasetId = new Date().getTime();

        createMockGetDataset(datasetId, { connectorType: 'foo' });

        const requestBody = {
            loggedUser: null
        };

        const query = `select * from ${datasetId}`;

        const queryResponse = await requester
            .post(`/api/v1/document/download/foo/${datasetId}?sql=${encodeURI(query)}`)
            .send(requestBody);

        queryResponse.status.should.equal(422);
        queryResponse.body.should.have.property('errors').and.be.an('array').and.have.lengthOf(1);
        queryResponse.body.errors[0].detail.should.include('This operation is only supported for datasets with provider [\'json\', \'csv\', \'tsv\', \'xml\']');
    });

    it('Download from a dataset without connectorType document should fail', async () => {
        const datasetId = new Date().getTime();

        createMockGetDataset(datasetId, { connectorType: 'foo' });

        const requestBody = {
            loggedUser: null
        };

        const query = `select * from ${datasetId}`;

        const queryResponse = await requester
            .post(`/api/v1/document/download/csv/${datasetId}?sql=${encodeURI(query)}`)
            .send(requestBody);

        queryResponse.status.should.equal(422);
        queryResponse.body.should.have.property('errors').and.be.an('array').and.have.lengthOf(1);
        queryResponse.body.errors[0].detail.should.include('This operation is only supported for datasets with connectorType \'document\'');
    });

    it('Download from a without a supported provider should fail', async () => {
        const datasetId = new Date().getTime();

        createMockGetDataset(datasetId, { provider: 'foo' });

        const requestBody = {
            loggedUser: null
        };

        const query = `select * from ${datasetId}`;

        const queryResponse = await requester
            .post(`/api/v1/document/download/csv/${datasetId}?sql=${encodeURI(query)}`)
            .send(requestBody);

        queryResponse.status.should.equal(422);
        queryResponse.body.should.have.property('errors').and.be.an('array').and.have.lengthOf(1);
        queryResponse.body.errors[0].detail.should.include('This operation is only supported for datasets with provider [\'json\', \'csv\', \'tsv\', \'xml\']');
    });

    it('Download with CSV format and a query that returns no results should be successful', async () => {
        const datasetId = new Date().getTime();

        createMockGetDataset(datasetId);

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

        await createIndex(
            'test_index_d1ced4227cd5480a8904d3410d75bf42_1587619728489',
            {
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
        );

        const response = await requester
            .post(`/api/v1/document/download/csv/${datasetId}`)
            .query({ sql: query })
            .send();

        response.status.should.equal(200);
        response.body.should.equal('');
        (await hasOpenScrolls()).should.equal(false);
    });

    it('Download with invalid format should return a 400', async () => {
        const datasetId = new Date().getTime();

        const response = await requester
            .post(`/api/v1/document/download/csv/${datasetId}`)
            .query({ sql: '', format: 'potato' })
            .send();

        response.status.should.equal(400);
        response.body.should.have.property('errors').and.be.an('array');
        response.body.errors[0].should.have.property('detail').and.equal(`- format: format must be in [json,csv]. - `);
        (await hasOpenScrolls()).should.equal(false);
    });

    afterEach(() => {
        deleteTestIndeces();

        if (!nock.isDone()) {
            const pendingMocks = nock.pendingMocks();
            if (pendingMocks.length > 1) {
                throw new Error(`Not all nock interceptors were used: ${pendingMocks}`);
            }
        }

    });
});

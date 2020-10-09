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

describe('Query datasets - GROUP BY queries', () => {

    before(async () => {
        await deleteTestIndeces();

        if (process.env.NODE_ENV !== 'test') {
            throw Error(`Running the test suite with NODE_ENV ${process.env.NODE_ENV} may result in permanent data loss. Please use NODE_ENV=test.`);
        }
    });

    it('Group by `column` query to dataset should be successful (happy case)', async () => {
        const datasetId = new Date().getTime();

        createMockGetDataset(datasetId);

        const requestBody = {
            loggedUser: null
        };

        const query = `select COUNT(*) as count, iso as country_iso from ${datasetId} GROUP BY iso`;

        const results = [
            { country_iso: 'ARM', count: 1 }, { country_iso: 'AUS', count: 1 }, {
                country_iso: 'AZE',
                count: 1
            }, { country_iso: 'BDI', count: 1 }, { country_iso: 'BIH', count: 1 }, {
                country_iso: 'BOL',
                count: 1
            }, { country_iso: 'BRA', count: 1 }, { country_iso: 'BTN', count: 1 }, {
                country_iso: 'CAN',
                count: 1
            }, { country_iso: 'CHE', count: 1 }, { country_iso: 'CHN', count: 1 }, {
                country_iso: 'CIV',
                count: 1
            }, { country_iso: 'CMR', count: 1 }, { country_iso: 'CRI', count: 1 }, {
                country_iso: 'ECU',
                count: 1
            }, { country_iso: 'GNB', count: 1 }, { country_iso: 'GRD', count: 1 }, {
                country_iso: 'HTI',
                count: 1
            }, { country_iso: 'IDN', count: 1 }, { country_iso: 'IRQ', count: 1 }, {
                country_iso: 'KAZ',
                count: 1
            }, { country_iso: 'KGZ', count: 1 }, { country_iso: 'KNA', count: 1 }, {
                country_iso: 'LBN',
                count: 1
            }, { country_iso: 'LIE', count: 1 }, { country_iso: 'LSO', count: 1 }, {
                country_iso: 'MAR',
                count: 1
            }, { country_iso: 'MEX', count: 1 }, { country_iso: 'MMR', count: 1 }, {
                country_iso: 'MOZ',
                count: 1
            }, { country_iso: 'MUS', count: 1 }, { country_iso: 'NER', count: 1 }, {
                country_iso: 'NGA',
                count: 1
            }, { country_iso: 'NZL', count: 1 }, { country_iso: 'PER', count: 1 }, {
                country_iso: 'PNG',
                count: 1
            }, { country_iso: 'PRK', count: 1 }, { country_iso: 'PRY', count: 1 }, {
                country_iso: 'RUS',
                count: 1
            }, { country_iso: 'RWA', count: 1 }, { country_iso: 'SDN', count: 1 }, {
                country_iso: 'SLE',
                count: 1
            }, { country_iso: 'SLV', count: 1 }, { country_iso: 'TGO', count: 1 }, {
                country_iso: 'TJK',
                count: 1
            }, { country_iso: 'UGA', count: 1 }, { country_iso: 'UKR', count: 1 }, {
                country_iso: 'USA',
                count: 1
            }, { country_iso: 'VCT', count: 1 }, { country_iso: 'VNM', count: 1 }, { country_iso: 'YEM', count: 1 }];

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
                    attributes: {
                        query: `SELECT COUNT(*) AS count, iso AS country_iso FROM ${datasetId} GROUP BY iso`,
                        jsonSql: {
                            select: [
                                {
                                    type: 'function',
                                    alias: 'count',
                                    value: 'COUNT',
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
                                    alias: 'country_iso',
                                    type: 'literal'
                                }
                            ],
                            from: `${datasetId}`,
                            group: [
                                {
                                    type: 'literal',
                                    value: 'iso'
                                }
                            ]
                        }
                    }
                }
            });

        await createIndex(
            'test_index_d1ced4227cd5480a8904d3410d75bf42_1587619728489',
            '_doc',
            {
                Country: { type: 'text', fields: { keyword: { type: 'keyword', ignore_above: 256 } } },
                'INDC-vs-NDC': {
                    type: 'text',
                    fields: { keyword: { type: 'keyword', ignore_above: 256 } }
                },
                iso: { type: 'text', fields: { keyword: { type: 'keyword', ignore_above: 256 } } },
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
        );

        await insertData(
            'test_index_d1ced4227cd5480a8904d3410d75bf42_1587619728489',
            '_doc',
            results.map(
                (e) => ({
                    Country: e.country_iso,
                    'INDC-vs-NDC': 'string',
                    iso: e.country_iso,
                    data_id: 'string',
                    'economyWide-Target': 'string',
                    'economyWide-Target-Description': 'string',
                    'landUse-GHG': 1,
                    'landUse-GHG-Description': 1,
                    'landUse-NonGHG': 'string',
                    'landUse-NonGHG-Description': 'string',
                    'landuse-excluded': 1,
                    'landuse-excluded-description': 1
                })
            )
        );

        const queryResponse = await requester
            .post(`/api/v1/document/query/${datasetId}?sql=${query}`)
            .send(requestBody);

        queryResponse.status.should.equal(200);
        queryResponse.body.should.have.property('data').and.be.an('array');
        queryResponse.body.should.have.property('meta').and.be.an('object');

        queryResponse.body.data.should.have.lengthOf(results.length);

        queryResponse.body.data.should.deep.equal(results.map((e) => ({ iso: e.country_iso, ...e })));
    });

    it('Group by `column` query with more than 10 results per group should be successful', async () => {
        const datasetId = new Date().getTime();

        createMockGetDataset(datasetId);

        const requestBody = {
            loggedUser: null
        };

        const query = `select SUM(count) as count, iso as country_iso from ${datasetId} GROUP BY iso`;

        const data = [
            { country_iso: 'ARM', count: 1 }, { country_iso: 'AUS', count: 1 }, {
                country_iso: 'AZE',
                count: 1
            }, { country_iso: 'BDI', count: 1 }, { country_iso: 'BIH', count: 1 }, {
                country_iso: 'BOL',
                count: 1
            }, { country_iso: 'BRA', count: 1 }, { country_iso: 'BTN', count: 1 }, {
                country_iso: 'CAN',
                count: 1
            }, { country_iso: 'CHE', count: 1 }, { country_iso: 'CHN', count: 1 }, {
                country_iso: 'CIV',
                count: 1
            }, { country_iso: 'CMR', count: 1 }, { country_iso: 'CRI', count: 1 }, {
                country_iso: 'ECU',
                count: 1
            }, { country_iso: 'GNB', count: 1 }, { country_iso: 'GRD', count: 1 }, {
                country_iso: 'HTI',
                count: 1
            }, { country_iso: 'IDN', count: 1 }, { country_iso: 'IRQ', count: 1 }, {
                country_iso: 'KAZ',
                count: 1
            }, { country_iso: 'KGZ', count: 1 }, { country_iso: 'KNA', count: 1 }, {
                country_iso: 'LBN',
                count: 1
            }, { country_iso: 'LIE', count: 1 }, { country_iso: 'LSO', count: 1 }, {
                country_iso: 'MAR',
                count: 1
            }, { country_iso: 'MEX', count: 1 }, { country_iso: 'MMR', count: 1 }, {
                country_iso: 'MOZ',
                count: 1
            }, { country_iso: 'MUS', count: 1 }, { country_iso: 'NER', count: 1 }, {
                country_iso: 'NGA',
                count: 1
            }, { country_iso: 'NZL', count: 1 }, { country_iso: 'PER', count: 1 }, {
                country_iso: 'PNG',
                count: 1
            }, { country_iso: 'PRK', count: 1 }, { country_iso: 'PRY', count: 1 }, {
                country_iso: 'RUS',
                count: 1
            }, { country_iso: 'RWA', count: 1 }, { country_iso: 'SDN', count: 1 }, {
                country_iso: 'SLE',
                count: 1
            }, { country_iso: 'SLV', count: 1 }, { country_iso: 'TGO', count: 1 }, {
                country_iso: 'TJK',
                count: 1
            }, { country_iso: 'UGA', count: 1 }, { country_iso: 'UKR', count: 1 }, {
                country_iso: 'USA',
                count: 1
            }, { country_iso: 'VCT', count: 1 }, { country_iso: 'VNM', count: 1 }, { country_iso: 'YEM', count: 1 }];

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
                    attributes: {
                        query: `SELECT COUNT(*) AS count, iso AS country_iso FROM ${datasetId} GROUP BY iso`,
                        jsonSql: {
                            select: [
                                {
                                    type: 'function',
                                    alias: 'count',
                                    value: 'COUNT',
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
                                    alias: 'country_iso',
                                    type: 'literal'
                                }
                            ],
                            from: `${datasetId}`,
                            group: [
                                {
                                    type: 'literal',
                                    value: 'iso'
                                }
                            ]
                        }
                    }
                }
            });

        await createIndex(
            'test_index_d1ced4227cd5480a8904d3410d75bf42_1587619728489',
            '_doc',
            {
                Country: { type: 'text', fields: { keyword: { type: 'keyword', ignore_above: 256 } } },
                'INDC-vs-NDC': {
                    type: 'text',
                    fields: { keyword: { type: 'keyword', ignore_above: 256 } }
                },
                iso: { type: 'text', fields: { keyword: { type: 'keyword', ignore_above: 256 } } },
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
        );

        await insertData(
            'test_index_d1ced4227cd5480a8904d3410d75bf42_1587619728489',
            '_doc',
            data.map(
                (e) => ({
                    Country: e.country_iso,
                    'INDC-vs-NDC': 'string',
                    iso: e.country_iso,
                    data_id: 'string',
                    'economyWide-Target': 'string',
                    'economyWide-Target-Description': 'string',
                    'landUse-GHG': 1,
                    'landUse-GHG-Description': 1,
                    'landUse-NonGHG': 'string',
                    'landUse-NonGHG-Description': 'string',
                    'landuse-excluded': 1,
                    'landuse-excluded-description': 1
                })
            )
        );

        const queryResponse = await requester
            .post(`/api/v1/document/query/${datasetId}?sql=${query}`)
            .send(requestBody);

        queryResponse.status.should.equal(200);
        queryResponse.body.should.have.property('data').and.be.an('array');
        queryResponse.body.should.have.property('meta').and.be.an('object');

        queryResponse.body.data.should.have.lengthOf(data.length);

        queryResponse.body.data.should.deep.equal(
            data.map((e) => ({
                iso: e.country_iso,
                count: 1,
                country_iso: e.country_iso
            }))
        );
    });

    it('Group by date part query to dataset should be successful', async () => {
        const datasetId = new Date().getTime();

        createMockGetDataset(datasetId);

        const query = `select createdAt from ${datasetId} group by date_histogram('field'="createdAt",'interval'='1d')`;

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
                    attributes: {
                        query: `SELECT createdAt FROM ${datasetId} GROUP BY date_histogram('field'="createdAt", 'interval'='1d')`,
                        jsonSql: {
                            select: [
                                {
                                    value: 'createdAt',
                                    alias: null,
                                    type: 'literal'
                                }
                            ],
                            from: 'foo',
                            group: [
                                {
                                    type: 'function',
                                    alias: null,
                                    value: 'date_histogram',
                                    arguments: [
                                        {
                                            name: 'field',
                                            type: 'literal',
                                            value: 'createdAt'
                                        },
                                        {
                                            name: 'interval',
                                            type: 'string',
                                            value: '1d'
                                        }
                                    ]
                                }
                            ]
                        }
                    }
                }
            });

        await createIndex(
            'test_index_d1ced4227cd5480a8904d3410d75bf42_1587619728489',
            '_doc',
            {
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
        );

        const dates = [
            '2018-09-07T00:00:01Z',
            '2018-09-08T00:00:01Z',
            '2018-09-09T00:00:01Z',
            '2018-09-10T00:00:01Z',
            '2018-09-11T00:00:01Z',
            '2018-09-12T00:00:01Z',
            '2018-09-13T00:00:01Z',
            '2018-09-14T00:00:01Z',
            '2018-09-15T00:00:01Z',
            '2018-09-16T00:00:01Z',
            '2018-09-17T00:00:01Z',
        ];

        await insertData(
            'test_index_d1ced4227cd5480a8904d3410d75bf42_1587619728489',
            '_doc',
            dates.map(
                (e) => ({
                    createdAt: e,
                    profilePictureUrl: 'string',
                    screenName: 'string',
                    text: 'string'
                })
            )
        );

        const queryResponse = await requester
            .post(`/api/v1/document/query/${datasetId}`)
            .query({
                sql: query
            })
            .send({
                loggedUser: null
            });

        queryResponse.status.should.equal(200);
        queryResponse.body.should.have.property('data').and.be.an('array');
        queryResponse.body.should.have.property('meta').and.be.an('object');

        queryResponse.body.data.should.have.lengthOf(results.length);

        queryResponse.body.data.should.deep.equal(results);
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

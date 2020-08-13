const nock = require('nock');

const createMockGetDataset = (id, anotherData = {}) => {
    nock(process.env.CT_URL)
        .get(`/v1/dataset/${id}`)
        .reply(200, {
            data: {
                id,
                type: 'dataset',
                attributes: {
                    name: 'Test dataset 1',
                    slug: 'test-dataset-1',
                    type: 'tabular',
                    subtitle: null,
                    application: [
                        'rw'
                    ],
                    dataPath: null,
                    attributesPath: null,
                    connectorType: 'document',
                    provider: 'csv',
                    userId: '1',
                    connectorUrl: 'https://raw.githubusercontent.com/test/file.csv',
                    sources: [],
                    tableName: 'index_d1ced4227cd5480a8904d3410d75bf42_1587619728489',
                    status: 'saved',
                    published: false,
                    overwrite: true,
                    mainDateField: null,
                    env: 'production',
                    geoInfo: false,
                    protected: false,
                    clonedHost: {},
                    legend: {},
                    errorMessage: null,
                    taskId: null,
                    createdAt: '2016-08-01T15:28:15.050Z',
                    updatedAt: '2018-01-05T18:15:23.266Z',
                    dataLastUpdated: null,
                    widgetRelevantProps: [],
                    layerRelevantProps: [],
                    ...anotherData
                }
            }
        });
};

module.exports = {
    createMockGetDataset
};

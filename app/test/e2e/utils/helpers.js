const nock = require('nock');
const config = require('config');
const { Client } = require('@elastic/elasticsearch');

const elasticSearchConfig = {
    node: config.get('elasticsearch.host')
};

if (config.get('elasticsearch.user') && config.get('elasticsearch.password')) {
    elasticSearchConfig.auth = {
        username: config.get('elasticsearch.user'),
        password: config.get('elasticsearch.password')
    };
}

const createMockGetDataset = (id, anotherData = {}) => {
    const dataset = {
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
            tableName: 'test_index_d1ced4227cd5480a8904d3410d75bf42_1587619728489',
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
    };

    nock(process.env.CT_URL)
        .get(`/v1/dataset/${id}`)
        .reply(200, {
            data: dataset
        });

    return dataset;
};

const createIndex = async (index, mappings) => {
    const body = {
        settings: {
            index: {
                number_of_shards: 1
            }
        },
        mappings: {
            properties: mappings
        }
    };

    const ESClient = new Client(elasticSearchConfig);

    const response = await ESClient.indices.create({
        index,
        body
    });

    return response.body;
};

const insertData = async (index, data) => {
    const ESClient = new Client(elasticSearchConfig);

    const body = data.flatMap((doc) => [{ index: { _index: index } }, doc]);

    return ESClient.bulk({ body, timeout: '90s', refresh: 'wait_for' });
};

const updateESConfig = async (data) => {
    const ESClient = new Client(elasticSearchConfig);

    return ESClient.cluster.putSettings({ body: data });
};

const deleteTestIndeces = async () => {
    const ESClient = new Client(elasticSearchConfig);

    const response = await ESClient.cat.indices({
        format: 'json'
    });

    const promises = response.body.map((index) => {
        if (index.index.startsWith('test_')) {
            return ESClient.indices.delete({
                index: index.index
            });
        }

        return new Promise(((resolve) => resolve()));
    });

    return Promise.all(promises);
};

const hasOpenScrolls = async () => {
    const client = new Client(elasticSearchConfig);
    const res = await client.nodes.stats({ level: 'indices', indexMetric: 'search' });
    const nScrolls = res.body.nodes[Object.keys(res.body.nodes)[0]].indices.search.scroll_current;
    return nScrolls > 0;
};

const mockGetUserFromToken = (userProfile) => {
    nock(process.env.CT_URL, { reqheaders: { authorization: 'Bearer abcd' } })
        .get('/auth/user/me')
        .reply(200, userProfile);
};

module.exports = {
    createMockGetDataset,
    createIndex,
    deleteTestIndeces,
    insertData,
    hasOpenScrolls,
    updateESConfig,
    mockGetUserFromToken
};

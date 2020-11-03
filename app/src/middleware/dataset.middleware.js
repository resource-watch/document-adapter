const logger = require('logger');
const DatasetService = require('services/datasetService');

class DatasetMiddleware {

    static async getDatasetById(ctx, next) {
        const datasetId = ctx.params.dataset;
        logger.debug('[DatasetRouter - getDatasetById] - Dataset id', datasetId);

        if (!datasetId) {
            ctx.throw(400, 'Invalid request');
        }

        const dataset = await DatasetService.getDatasetById(datasetId);

        if (!dataset) {
            ctx.throw(404, 'Dataset not found');
        }

        if (dataset.attributes.connectorType !== 'document') {
            ctx.throw(422, 'This operation is only supported for datasets with connectorType \'document\'');
        }

        if (!['json', 'csv', 'tsv', 'xml'].includes(dataset.attributes.provider)) {
            ctx.throw(422, 'This operation is only supported for datasets with provider [\'json\', \'csv\', \'tsv\', \'xml\']');
        }

        ctx.request.body.dataset = {
            id: dataset.id,
            ...dataset.attributes
        };

        await next();
    }

    static async validateDatasetProvider(ctx, next) {
        const { provider } = ctx.params;

        logger.debug('[DatasetRouter - validateDatasetProvider] - Dataset provider', provider);

        if (!['json', 'csv', 'tsv', 'xml'].includes(provider)) {
            ctx.throw(422, 'This operation is only supported for datasets with provider [\'json\', \'csv\', \'tsv\', \'xml\']');
        }

        await next();
    }

}

module.exports = DatasetMiddleware;

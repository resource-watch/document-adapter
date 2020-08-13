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
            ctx.throw(422, 'This operation is only supported for datasets with type \'document\'');
        }

        ctx.request.body.dataset = {
            id: dataset.id,
            ...dataset.attributes
        };

        await next();
    }

}

module.exports = DatasetMiddleware;

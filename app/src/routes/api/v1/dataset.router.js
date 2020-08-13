const logger = require('logger');
const Router = require('koa-router');
const taskQueueService = require('services/taskQueueService');
const DatasetService = require('services/datasetService');
const ctRegisterMicroservice = require('ct-register-microservice-node');

const router = new Router({
    prefix: '/document'
});

class DatasetRouter {

    static async import(ctx) {
        logger.info('Adding dataset with dataset id: ', ctx.request.body);
        await taskQueueService.import({
            datasetId: ctx.request.body.connector.id,
            fileUrl: ctx.request.body.connector.sources,
            data: ctx.request.body.connector.data,
            dataPath: ctx.request.body.connector.dataPath,
            provider: ctx.params.provider || 'csv',
            legend: ctx.request.body.connector.legend,
            verified: ctx.request.body.connector.verified
        });
        ctx.body = '';
    }

    static async overwrite(ctx) {
        logger.info('Overwrite dataset with dataset id: ', ctx.params.dataset);
        ctx.assert(ctx.request.body.url || ctx.request.body.data || ctx.request.body.sources, 400, 'Url or data is required');
        ctx.assert(ctx.request.body.provider, 400, 'Provider required');
        if (ctx.request.body.dataset && (ctx.request.body.dataset.status !== 'saved' && ctx.request.body.dataset.status !== 'failed')) {
            ctx.throw(400, 'Dataset is not in saved status');
            return;
        }

        await taskQueueService.overwrite({
            datasetId: ctx.params.dataset,
            fileUrl: ctx.request.body.sources || [ctx.request.body.url],
            data: ctx.request.body.data,
            dataPath: ctx.request.body.dataPath,
            provider: ctx.request.body.provider || 'csv',
            legend: ctx.request.body.legend,
            index: ctx.request.body.dataset.tableName
        });
        ctx.set('cache-control', 'flush');
        ctx.body = '';
    }

    static async concat(ctx) {
        logger.info('Concat dataset with dataset id: ', ctx.params.dataset);
        ctx.assert(ctx.request.body.url || ctx.request.body.data || ctx.request.body.sources, 400, 'Url or data is required');
        ctx.assert(ctx.request.body.provider, 400, 'Provider required');
        if (ctx.request.body.dataset && (ctx.request.body.dataset.status !== 'saved' && ctx.request.body.dataset.status !== 'failed')) {
            ctx.throw(400, 'Dataset is not in saved status');
            return;
        }
        await taskQueueService.concat({
            datasetId: ctx.params.dataset,
            fileUrl: ctx.request.body.sources || [ctx.request.body.url],
            data: ctx.request.body.data,
            dataPath: ctx.request.body.dataPath,
            provider: ctx.request.body.provider || 'csv',
            legend: ctx.request.body.dataset.legend,
            index: ctx.request.body.dataset.tableName
        });
        ctx.set('cache-control', 'flush');
        ctx.body = '';
    }

    static async append(ctx) {
        logger.info('Concat dataset with dataset id: ', ctx.params.dataset);
        ctx.assert(ctx.request.body.url || ctx.request.body.data || ctx.request.body.sources, 400, 'Url or data is required');
        ctx.assert(ctx.request.body.provider, 400, 'Provider required');
        if (ctx.request.body.dataset && (ctx.request.body.dataset.status !== 'saved' && ctx.request.body.dataset.status !== 'failed')) {
            ctx.throw(400, 'Dataset is not in saved status');
            return;
        }
        await taskQueueService.append({
            datasetId: ctx.params.dataset,
            fileUrl: ctx.request.body.sources || [ctx.request.body.url],
            data: ctx.request.body.data,
            dataPath: ctx.request.body.dataPath,
            provider: ctx.request.body.provider || 'csv',
            legend: ctx.request.body.dataset.legend,
            index: ctx.request.body.dataset.tableName
        });
        ctx.set('cache-control', 'flush');
        ctx.body = '';
    }

    static async deleteIndex(ctx) {
        logger.info('Deleting index with dataset', ctx.request.body);
        const response = await ctRegisterMicroservice.requestToMicroservice({
            method: 'GET',
            json: true,
            uri: `/dataset/${ctx.params.dataset}`
        });

        if (response.data.attributes.tableName) {
            await taskQueueService.deleteIndex({
                datasetId: ctx.params.dataset,
                index: response.data.attributes.tableName
            });
        }
        ctx.set('cache-control', 'flush');
        ctx.body = '';
    }

}

const containApps = (apps1, apps2) => {
    if (!apps1 || !apps2) {
        return false;
    }
    for (let i = 0, { length } = apps1; i < length; i += 1) {
        for (let j = 0, length2 = apps2.length; j < length2; j += 1) {
            if (apps1[i] === apps2[j]) {
                return true;
            }
        }
    }
    return false;
};

const checkUserHasPermission = (user, dataset) => {
    if (user && dataset) {
        if (user.id === 'microservice') {
            return true;
        }
        // check if user is admin of any application of the dataset or manager and owner of the dataset
        if (user.role === 'MANAGER' && user.id === dataset.userId) {
            return true;
        }
        if (user.role === 'ADMIN' && containApps(dataset.application, user.extraUserData ? user.extraUserData.apps : null)) {
            return true;
        }

    }
    return false;
};

const checkPermissionModify = async (ctx, next) => {
    logger.debug('Checking if the user has permissions');
    const user = ctx.request.body.loggedUser;
    const { dataset } = ctx.request.body;
    if (!user) {
        logger.debug(`User data missing`);
        ctx.throw(401, 'User credentials invalid or missing');
    }
    if (!dataset) {
        ctx.throw(400, 'Dataset not found');
    }
    if (checkUserHasPermission(user, dataset)) {
        if (dataset.overwrite) {
            await next();
            return;
        }
        ctx.throw(409, 'Dataset locked. Overwrite false.');
    } else {
        logger.debug(`User ${user.id} with role ${user.role} and app(s) '${(user.extraUserData ? user.extraUserData.apps : []).join(', ')}' does not have permissions to modify dataset ${dataset.id}`);
        ctx.throw(403, 'Not authorized');
    }
};

const getDatasetById = async (ctx, next) => {
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
};

router.post('/:provider', DatasetRouter.import);
router.post('/:dataset/data-overwrite', getDatasetById, checkPermissionModify, DatasetRouter.overwrite);
router.post('/:dataset/concat', getDatasetById, checkPermissionModify, DatasetRouter.concat);
router.post('/:dataset/append', getDatasetById, checkPermissionModify, DatasetRouter.append);
router.delete('/:dataset', DatasetRouter.deleteIndex);

module.exports = router;

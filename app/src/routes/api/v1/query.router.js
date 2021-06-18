const Router = require('koa-router');
const logger = require('logger');
const queryService = require('services/queryService');
const taskQueueService = require('services/taskQueueService');
const fieldSerializer = require('serializers/fieldSerializer');
const Json2sql = require('sql2json').json2sql;
const passThrough = require('stream').PassThrough;
const DownloadValidator = require('validators/download.validator');
const DatasetMiddleware = require('middleware/dataset.middleware');
const { ElasticsearchClientError } = require('@elastic/elasticsearch/lib/errors');

const { RWAPIMicroservice } = require('rw-api-microservice-node');

const router = new Router({
    prefix: '/document'
});

const serializeObjToQuery = (obj) => Object.keys(obj).reduce((a, k) => {
    a.push(`${k}=${encodeURIComponent(obj[k])}`);
    return a;
}, []).join('&');

class QueryRouter {

    static async query(ctx) {
        logger.info('Do Query with dataset', ctx.request.body);
        logger.debug('Checking if is delete or select');

        try {
            if (ctx.state.parsed.delete) {
                logger.debug('Doing delete');
                ctx.state.parsed.from = ctx.request.body.dataset.tableName;
                const sql = Json2sql.toSQL(ctx.state.parsed);
                ctx.body = await taskQueueService.delete({
                    datasetId: ctx.params.dataset,
                    query: sql,
                    index: ctx.request.body.dataset.tableName
                });
            } else if (ctx.state.parsed.select) {
                ctx.body = passThrough();
                const cloneUrl = QueryRouter.getCloneUrl(ctx.request.url, ctx.params.dataset);
                ctx.state.parsed.from = ctx.request.body.dataset.tableName;
                const sql = Json2sql.toSQL(ctx.state.parsed);
                logger.debug(ctx.request.body.dataset);
                logger.debug('ElasticSearch query', sql);
                await queryService.doQuery(sql, ctx.state.parsed, ctx.request.body.dataset.tableName, ctx.params.dataset, ctx.body, cloneUrl, ctx.query.format);
            } else {
                ctx.throw(400, 'Query not valid');
            }
        } catch (err) {
            logger.error(err);
            if (err instanceof ElasticsearchClientError && err.meta && err.meta.body) {
                throw new Error(`${err.message} - ${JSON.stringify(err.meta.body)}`);
            }
            throw err;
        }
    }

    static async download(ctx) {
        ctx.body = passThrough();
        const format = ctx.query.format ? ctx.query.format : 'csv';
        ctx.set('Content-disposition', `attachment; filename=${ctx.params.dataset}.${format}`);
        let mimetype;
        switch (format) {

            case 'csv':
                mimetype = 'text/csv';
                break;
            case 'json':
            case 'geojson':
                mimetype = 'application/json';
                break;
            default:
                logger.debug('default');

        }
        ctx.set('Content-type', mimetype);
        ctx.state.parsed.from = ctx.request.body.dataset.tableName;
        const sql = Json2sql.toSQL(ctx.state.parsed);
        await queryService.downloadQuery(sql, ctx.state.parsed, ctx.request.body.dataset.tableName, ctx.request.body.dataset.id, ctx.body, format);
    }

    static async fields(ctx) {
        logger.info('Get fields of dataset', ctx.request.body);
        const mapping = await queryService.getMapping(ctx.request.body.dataset.tableName);
        ctx.body = fieldSerializer.serialize(mapping, ctx.request.body.dataset.tableName, ctx.request.body.dataset.id);
    }

    static getCloneUrl(url, idDataset) {
        return {
            http_method: 'POST',
            url: `/v1/dataset/${idDataset}/clone`,
            body: {
                dataset: {
                    datasetUrl: `/v1${url.replace('/document', '')}`,
                    application: ['your', 'apps']
                }
            }
        };
    }

}

const toSQLMiddleware = async (ctx, next) => {

    const options = {
        method: 'GET',
        json: true
    };
    if (!ctx.query.sql && !ctx.request.body.sql && !ctx.query.outFields && !ctx.query.outStatistics) {
        ctx.throw(400, 'sql or fs required');
        return;
    }

    if (ctx.query.sql || ctx.request.body.sql) {
        logger.debug('Checking sql correct');
        const params = { ...ctx.query, ...ctx.request.body };
        options.uri = `/v1/convert/sql2SQL?sql=${encodeURIComponent(params.sql)}`;
        if (params.experimental) {
            options.uri += `&experimental=${params.experimental}`;
        }
        if (params.geostore) {
            options.uri += `&geostore=${params.geostore}`;
        }
        if (params.geojson) {
            options.body = {
                geojson: params.geojson
            };
            options.method = 'POST';
        }
    } else {
        logger.debug('Obtaining sql from featureService');
        const fs = { ...ctx.request.body };
        delete fs.dataset;
        const query = serializeObjToQuery(ctx.request.query);
        const body = fs;
        const resultQuery = { ...query };

        if (resultQuery) {
            options.uri = `/v1/convert/fs2SQL${resultQuery}'&tableName=${ctx.request.body.dataset.tableName}`;
        } else {
            options.uri = `/v1/convert/fs2SQL?tableName=${ctx.request.body.dataset.tableName}`;
        }
        options.body = body;
        options.method = 'POST';
    }

    logger.debug(options);
    try {
        const result = await RWAPIMicroservice.requestToMicroservice(options);
        ctx.query.sql = result.data.attributes.query;
        ctx.state.parsed = result.data.attributes.jsonSql;
        logger.debug(ctx.query.sql);
    } catch (e) {
        logger.warn(`Could not issue request to MS: ${options.method} ${options.uri} with message ${e.message}`);
        if (e.statusCode === 400 || e.statusCode === 404) {
            ctx.status = e.statusCode;
            ctx.body = e.body;
        }
        throw e;
    }
    await next();
};

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

const checkPermissionDelete = async (ctx, next) => {
    if (ctx.state.parsed.delete) {
        if (!ctx.request.body || !ctx.request.body.loggedUser) {
            ctx.throw(403, 'Not authorized to execute DELETE query');
            return;
        }
        if (ctx.request.body && ctx.request.body.loggedUser) {

            if (checkUserHasPermission(ctx.request.body.loggedUser, ctx.request.body.dataset)) {
                await next();
                return;
            }
            ctx.throw(403, 'Not authorized to execute DELETE query');
            return;
        }
    }
    await next();
};

router.get('/query/:provider/:dataset', DatasetMiddleware.validateDatasetProvider, DatasetMiddleware.getDatasetById, toSQLMiddleware, checkPermissionDelete, QueryRouter.query);
router.post('/query/:provider/:dataset', DatasetMiddleware.validateDatasetProvider, DatasetMiddleware.getDatasetById, toSQLMiddleware, checkPermissionDelete, QueryRouter.query);
router.get('/download/:provider/:dataset', DatasetMiddleware.validateDatasetProvider, DownloadValidator.validateDownload, DatasetMiddleware.getDatasetById, toSQLMiddleware, QueryRouter.download);
router.post('/download/:provider/:dataset', DatasetMiddleware.validateDatasetProvider, DownloadValidator.validateDownload, DatasetMiddleware.getDatasetById, toSQLMiddleware, QueryRouter.download);
router.get('/fields/:provider/:dataset', DatasetMiddleware.validateDatasetProvider, DatasetMiddleware.getDatasetById, QueryRouter.fields);

module.exports = router;

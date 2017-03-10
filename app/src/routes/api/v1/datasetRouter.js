'use strict';

const logger = require('logger');
const Router = require('koa-router');
const queueService = require('services/queueService');
const queryService = require('services/queryService');

const JSONAPIDeserializer = require('jsonapi-serializer').Deserializer;

const deserializer = function (obj) {
    return function (callback) {
        new JSONAPIDeserializer({
            keyForAttribute: 'camelCase'
        }).deserialize(obj, callback);
    };
};

const router = new Router({
    prefix: '/document'
});

class DatasetRouter {

    static * import () {
            logger.info('Adding dataset with dataset id: ', this.request.body);
            
            yield queueService.addDataset(this.params.provider || 'csv', this.request.body.connectorUrl, this.request.body.connector.data, 'index_' + this.request.body.connector.id.replace(/-/g, ''), this.request.body.connector.id, this.request.body.connector.legend, this.request.body.connector.data_path);
            this.body = '';
        }

    static * updateData() {
        logger.info(`Update data with id ${this.params.id}  of dataset ${this.request.body.dataset.id}`);
        this.assert(this.request.body.data, 400, 'Data is required');
        const result = yield queryService.updateElement(this.request.body.dataset.tableName, this.params.id, this.request.body.data);

        this.body = null;
    }

    static * overwrite() {
        logger.info('Overwrite dataset with dataset id: ', this.params.id);
        this.assert(this.request.body.url || this.request.body.data, 400, 'Url or data is required');
        this.assert(this.request.body.provider, 400, 'Provider required');
        
        yield queueService.overwriteDataset(this.request.body.provider, this.request.body.url, this.request.body.data, this.request.body.dataset.tableName, this.request.body.dataset.id, this.request.body.legend, this.request.body.dataPath);
        this.body = '';
    }

    static * concat() {
        logger.info('Concat dataset with dataset id: ', this.params.dataset);
        this.assert(this.request.body.url || this.request.body.data, 400, 'Url or data is required');
        this.assert(this.request.body.provider, 400, 'Provider required');
        
        yield queueService.concatDataset(this.request.body.provider, this.request.body.url, this.request.body.data, this.request.body.dataset.tableName, this.request.body.dataset.id, this.request.body.legend, this.request.body.dataPath);
        this.body = '';
    }

    static * delete() {
        logger.info('Deleting index with dataset', this.request.body);
        let result = yield queueService.deleteDataset('index_' + this.params.id.replace(/-/g, ''), this.params.id);
        this.body = result;
    }

}


const containApps = function (apps1, apps2) {
    if (!apps1 || !apps2) {
        return false;
    }
    for (let i = 0, length = apps1.length; i < length; i++)  {
        for (let j = 0, length2 = apps2.length; j < length2; j++) {
            if (apps1[i] === apps2[j]) {
                return true;
            }
        }
    }
    return false;
};

const checkUserHasPermission = function (user, dataset) {
    if (user && dataset) {
        // check if user is admin of any application of the dataset or manager and owner of the dataset
        if (user.role === 'MANAGER' && user.id === dataset.userId) {
            return true;
        } else if (user.role === 'ADMIN' && containApps(dataset.application, user.extraUserData ? user.extraUserData.apps : null)) {
            return true;
        }

    }
    return false;
};

const checkPermissionModify = function* (next) {
    logger.debug('Checking if the user has permissions');
    const user = this.request.body.loggedUser;
    const dataset = this.request.body.dataset;
    if (checkUserHasPermission(user, dataset)) {
        if (dataset.overwrite) {
            yield next;
            return;
        }
        this.throw(409, 'Dataset locked. Overwrite false.');
        return;

    } else {
        this.throw(403, 'Not authorized');
        return;
    }
};

const deserializeDataset = function* (next) {
    logger.debug('Body', this.request.body);
    if (this.request.body.dataset && this.request.body.dataset.data) {
        this.request.body.dataset = yield deserializer(this.request.body.dataset);
    } else {
        if (this.request.body.dataset && this.request.body.dataset.table_name) {
            this.request.body.dataset.tableName = this.request.body.dataset.table_name;
        }
    }
    yield next;
};

router.post('/:provider', DatasetRouter.import);
router.post('/data/:dataset/:id', deserializeDataset, DatasetRouter.updateData);
router.post('/:id/data-overwrite', deserializeDataset, checkPermissionModify, DatasetRouter.overwrite);
router.post('/concat/:dataset', deserializeDataset, checkPermissionModify, DatasetRouter.concat);
router.delete('/:id', DatasetRouter.delete);
module.exports = router;

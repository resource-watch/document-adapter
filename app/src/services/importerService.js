'use strict';

const logger = require('logger');
const config = require('config');
const queue = require('bull');
const fs = require('fs');
const co = require('co');
const DownloadService = require('services/downloadService');
const queryService = require('services/queryService');

const CSVImporter = require('services/csvImporter');
var microserviceClient = null;
var unlink = function(file) {
    return function(callback) {
        fs.unlink(file, callback);
    };
};

var getKey = function(key){
    return function(cb){
        require('redis').createClient({port: config.get('redis.port'), host:config.get('redis.host')}).get(key, cb);
    };
};

class ImporterService {
    constructor() {
        logger.info('Creating queue');
        this.importQueue = queue('importer', config.get('redis.port'), config.get('redis.host'));

        this.importQueue.on('on', function(err) {
            logger.error('Error in importqueue', err);
        });
        this.deleteQueue = queue('delete', config.get('redis.port'), config.get('redis.host'));

        this.importQueue.on('error', function(error) {
            logger.error('Error in queue', error);
        });
        this.importQueue.on('failed', function(error) {
            logger.error('Error in queue', error);
        });
        this.importQueue.on('completed', function(error) {
            logger.info('Completed');
        });
    }

    * updateState(id,  state, tableName) {
        logger.info('Updating state of dataset ', id, ' with status ', state);
        let data = yield getKey('MICROSERVICE_CONFIG');

        data = JSON.parse(data);
        let microserviceClient = require('vizz.microservice-client');
        microserviceClient.setDataConnection(data);
        let options = {
            uri: '/datasets/' + id,
            body: {
                dataset: {
                    dataset_attributes: {
                        status: state
                    }
                }
            },
            method: 'PUT',
            json: true
        };
        if(tableName){
            options.body.dataset.table_name = tableName;
        }
        logger.info('Updating', options);
        let result = yield microserviceClient.requestToMicroservice(options);
        if (result.statusCode !== 200) {
            logger.error('Error to updating dataset.', result);
            throw new Error('Error to updating dataset');
        }
    }

    addProcess() {
        //this processes are executed in separated forks
        this.importQueue.process(this.processImport.bind(this));
        this.deleteQueue.process(this.processDelete.bind(this));
    }

    * addCSV(url, index, id) {
        logger.info('Adding import csv task with url', url, ' and index ', index, ' and id ', id);
        this.importQueue.add({
            url: url,
            index: index,
            id: id
        }, {
            attempts: 3,
            timeout: 1800000, //5 minutes
            delay: 1000
        });
    }

    * deleteCSV(index, id) {
        logger.info('Adding delete csv task with id %id and index %s', id, index);
        this.deleteQueue.add({
            index: index,
            id: id
        }, {
            attempts: 3,
            timeout: 300000, //5 minutes
            delay: 1000
        });
    }

    * loadCSVInDatabase(path, index) {
        logger.info('Importing csv in path %s and index %s', path, index);
        let importer = new CSVImporter(path, index, index);
        yield importer.start();
    }

    processDelete(job, done) {
        logger.info('Proccesing delete task with index: %s and id: %s', job.data.index, job.data.id);
        co(function*() {
            logger.debug('Job', job);
            yield queryService.deleteIndex(job.data.index);
            logger.info('Deleted successfully. Updating state');
            yield this.updateState(job.data.id, 3);
        }.bind(this)).then(function() {
            logger.info('Finished deleted task successfully');
            done();
        }, function(error) {
            logger.error(error);
            done(new Error(error));
        });
    }

    processImport(job, done) {
        logger.info('Proccesing import task with url: %s and index: %s and id: %s', job.data.url, job.data.index, job.data.id);
        co(function*() {
            let path = null;
            logger.debug('Job', job);
            try {
                path = yield DownloadService.downloadFile(job.data.url);
                yield this.loadCSVInDatabase(path, job.data.index);
                logger.info('Imported successfully. Updating state');
                yield this.updateState(job.data.id, 1, job.data.index);
            } catch (err) {
                logger.error(err);
                throw err;
            } finally {
                logger.debug('Deleting file');
                if (path) {
                    yield unlink(path);
                }
            }

        }.bind(this)).then(function() {
            logger.info('Finished imported task successfully');
            done();
        }, function(error) {
            logger.error(error);
            done(new Error(error));
        });
    }
}

module.exports = new ImporterService();

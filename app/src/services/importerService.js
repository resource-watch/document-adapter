'use strict';

const logger = require('logger');
const config = require('config');
const queue = require('bull');
const ElasticsearchCSV = require('elasticsearch-csv');
const fs = require('fs');
const co = require('co');
const DownloadService = require('services/downloadService');

var unlink = function(file) {
    return function(callback) {
        fs.unlink(file, callback);
    };
};



class ImporterService {
    constructor() {
        logger.info('Creating queue');
        this.importQueue = queue('importer', config.get('redis.port'), config.get('redis.host'));
        this.importQueue.process(this.processImport.bind(this));
        this.importQueue.empty();
        this.importQueue.on('error', function(error) {
            logger.error('Error in queue', error);
        });
        this.importQueue.on('completed', function(error) {
            logger.info('Completed');
        });
    }

    * addTask(url, index, id) {
        logger.info('Adding task with message', url, ' and index ', index, ' and id ', id);
        this.importQueue.add({
            url: url,
            index: index,
            id: id
        }, {
            attempts: 3,
            timeout: 300000, //5 minutes
            delay: 1000
        });
    }

    * loadCSVInDatabase(path, index) {
        logger.debug('Importing csv in path %s and index %s', path, index);
        var esCSV = new ElasticsearchCSV({
            es: {
                index: index,
                type: index,
                host: config.get('elasticsearch.host') + ':' + config.get('elasticsearch.port')
            },
            csv: {
                filePath: path,
                headers: true
            }
        });
        let result = yield esCSV.import();

    }

    processImport(job, done) {
        logger.debug('Entra en el import');
        co(function*() {
            let path = null;
            logger.info('Proccesing task with url: %s and index: %s and id: %s', job.data.url, job.data.index, job.data.id);
            try {
                path = yield DownloadService.downloadFile(job.data.url);
                yield this.loadCSVInDatabase(path, job.data.index);
                logger.info('Imported successfully. Updating state');
                let result = yield require('microservice-client').requestToMicroservice({
                    uri: '/datasets/' + job.data.id,
                    body: {
                        dataset: {
                            dataset_attributes: {
                                status: 1
                            }
                        }
                    },
                    method: 'PUT',
                    json: true
                });
                if (result.statusCode !== 200) {
                    logger.error('Error to updating dataset.', result);
                    throw new Error('Error to updating dataset');
                }
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
            logger.info('Finished task successfully');
            done();
        }, function(error) {
            logger.error(error);
            done(new Error(error));
        });


    }
}

module.exports = new ImporterService();

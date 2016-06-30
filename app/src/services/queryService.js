'use strict';

const logger = require('logger');
const config = require('config');
const elasticsearch = require('elasticsearch');

class QueryService {

    constructor() {
        logger.info('Connecting with elasticsearch');

        var sqlAPI = {
            sql: function(opts) {
                return function(cb) {
                    this.transport.request({
                        method: 'GET',
                        path: encodeURI('/_sql'),
                        query: `sql=${encodeURI(opts.sql)}`
                    }, cb);
                }.bind(this);
            },
            mapping: function(opts) {
                return function(cb) {
                    this.transport.request({
                        method: 'GET',
                        path: `${opts.index}/_mapping`
                    }, cb);
                }.bind(this);
            },
            delete: function(opts){
                return function(cb) {
                    this.transport.request({
                        method: 'DELETE',
                        path: `${opts.index}`
                    }, cb);
                }.bind(this);
            }
        };
        elasticsearch.Client.apis.sql = sqlAPI;

        this.elasticClient = new elasticsearch.Client({
            host: config.get('elasticsearch.host') + ':' + config.get('elasticsearch.port'),
            log: 'info',
            apiVersion: 'sql'
        });

    }

    parseNormalStatement(parts, symbol){
        return `${parts[0]} ${symbol} ${parts[1]}`;
    }

    parseEquality(parts){
        let inParts = null;
        if((inParts = parts[1].split(',')).length > 1){
            //in
            return `${parts[0]} IN (${inParts.join(',')})`;
        } else {
            return `${parts[0]} = ${parts[1]}`;
        }
    }

    parseBetween(parts){
        let betParts = parts[1].split('..');
        return `${parts[0]} BETWEEN ${betParts[0]} AND ${betParts[1]}`;

    }

    parseStatement(expr){
        if(expr.indexOf('==') > -1){
            return this.parseEquality(expr.split('=='));
        } else if(expr.indexOf('>=') > -1){
            return this.parseNormalStatement(expr.split('>='), '>=');
        } else if(expr.indexOf('>>') > -1){
            return this.parseNormalStatement(expr.split('>>'), '>');
        } else if(expr.indexOf('<<') > -1){
            return this.parseNormalStatement(expr.split('<<'), '<');
        } else if(expr.indexOf('<=') > -1){
            return this.parseNormalStatement(expr.split('<='), '<=');
        } else if(expr.indexOf('><') > -1){
            return this.parseBetween(expr.split('><'));
        }
    }

    parseFilter(filter){
        let result = '';
        let parts = filter.split(/<and>|<or>/g);
        let partsWithOp = [];
        let lengthParts = parts.length;
        for(let i = 0; i < lengthParts; i++){
            partsWithOp.push(parts[i]);
            let sum = 0;
            for(let j = 0; j < (2*i+1); j++){
                sum += partsWithOp[j].length;
            }
            if(sum < filter.length){
                let oper = null;
                let indexOfAnd = filter.indexOf('<and>', sum);
                let indexOfOr = filter.indexOf('<or>', sum);
                if(indexOfAnd > -1 || indexOfOr > -1){
                    if(indexOfAnd > -1){
                        oper = '<and>';
                    } else{
                        oper = '<or>';
                    }
                } else if(indexOfAnd > -1 && indexOfOr > -1){
                    if(indexOfAnd <= indexOfOr){
                        oper = '<and>';
                    } else if(indexOfOr < indexOfAnd){
                        oper = '<or>';
                    }
                }
                if(oper){
                    partsWithOp.push(oper);
                }
            }
        }
        let where = '';
        for(let i=0, length=partsWithOp.length; i < length; i++){
            switch (partsWithOp[i].trim()) {
                case '<and>':
                    where +=' AND ';
                    break;
                case '<or>':
                    where += ' OR ';
                    break;
                default:
                    where += this.parseStatement(partsWithOp[i].trim());
                    break;
            }
        }
        return where;
    }

    parseSelect(select, aggrColumns){
        let result = '';
        if(!select && !aggrColumns){
            return '*';
        }
        if(select){
            result = select.join(', ');
        }
        if(result && aggrColumns){
            result +=', ';
        }
        if(aggrColumns){
            result += aggrColumns.join(', ');
        }
        return result;
    }

    convertToSQL(select, order, aggrBy, filter, filterNot, limit, aggrColums, tableName) {
        if(select){
            select = [].concat(select);
        }
        if(order){
            order = [].concat(order);
        }
        if(aggrBy){
            aggrBy = [].concat(aggrBy);
        }
        if(aggrColums){
            aggrColums = [].concat(aggrColums);
        }

        let whereStatement = '';
        if(filter){
            whereStatement = this.parseFilter(filter);
        }
        let whereNotStatement = '';
        if(filterNot){
            whereNotStatement = this.parseFilter(filterNot);
        }

        let selectStatement = this.parseSelect(select, aggrColums);

        let result = `SELECT
            ${ selectStatement }
            FROM ${tableName}
            ${(whereStatement || whereNotStatement) ? 'WHERE' : ''}
            ${whereStatement ? `${whereStatement}`: ''}
            ${whereNotStatement ? `NOT (${whereNotStatement})`: ''}
            ${(order && order.length > 0) ? `ORDER BY ${order.map(function(value){
                if(value.startsWith('-')){
                    return value.substring(1, value.length) + ' DESC';
                }
                return value + ' ASC';
            }).join(', ')}` : '' }
            ${aggrBy && aggrBy.length > 0 ? `GROUP BY ${aggrBy.join(', ')}`: ''}
            ${(limit && !isNaN(limit) && limit > 0) ? `LIMIT ${limit}` : ''}`;


        return result.replace(/\s\s+/g, ' ').trim();
    }

    * doQuery(select, order, aggrBy, filter, filterNot, limit, aggrColumns, index, sql){
        logger.info('Doing query...', aggrBy);
        let sqlGen = sql;
        if(!sqlGen){
            sqlGen = this.convertToSQL(select, order, aggrBy, filter, filterNot, limit, aggrColumns, index);
        }
        logger.debug('Query ', sqlGen);
        let result = yield this.elasticClient.sql({sql: sqlGen});
        return result;
    }

    * getMapping(index){
        logger.info('Obtaining mapping...');

        let result = yield this.elasticClient.mapping({index: index});
        return result;
    }

    * deleteIndex(index){
        logger.info('Deleting index %s...', index);

        let result = yield this.elasticClient.delete({index: index});
        return result;
    }
}

module.exports = new QueryService();

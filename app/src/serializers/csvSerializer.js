'use strict';

var logger = require('logger');
const Json2sql = require('sql2json').json2sql;
// var JSONAPISerializer = require('jsonapi-serializer').Serializer;
// var csvSerializer = new JSONAPISerializer('csv', {
// attributes: ['geojson', 'hash', 'providers'],
// geojson:{
//     attributes:['type', 'features', 'crs']
// },
// providers:{
//     attributes: ['provider', 'table', 'user']
// },
// typeForAttribute: function (attribute, record) {
//     return attribute;
// }
// });

class CSVSerializer {

    static serializeBucket(key, buckets) {
        let list = [];
        logger.debug('Entering with key', key, buckets);
        let alias = key;
        if (key.indexOf('geohash') >= 0){
            alias = 'geohash';
        }
        for (let i = 0, length = buckets.length; i < length; i++) {
            let keys = Object.keys(buckets[i]).filter((el) => el !== 'doc_count' && el !== 'key');
            if (keys.length === 1 && buckets[i][keys[0]].buckets) {
                const partialList = CSVSerializer.serializeBucket(keys[0], buckets[i][keys[0]].buckets);
                for (let j = 0, length = partialList.length; j < length; j++){
                    partialList[j][alias] = buckets[i].key;
                }
                list = list.concat(partialList);
            } else {
                let obj = {
                    [alias]: buckets[i].key
                };
                for (let j = 0, lengthSublist = keys.length; j < lengthSublist; j++){
                    if (buckets[i][keys[j]].value) {
                        obj[keys[j]]= buckets[i][keys[j]].value;
                    }
                }
                list.push(obj);
            }
        }
        return list;
    }

    // static serializeBucket(key, buckets, parsed) {
    //     let values = [];
    //     buckets.map((el) => {
    //         let value = el.key;

    //         if (Object.keys(el).length > 2) {
    //             let newKey = null;
    //             let keys = Object.keys(el);
    //             newKey = keys.filter((key) => (key!== 'key' && key !== 'doc_count'));

    //             if(newKey && el[newKey] && el[newKey].buckets){

    //                 let childs = CSVSerializer.serializeBucket(newKey, el[newKey].buckets);

    //                 childs.map((child) => {
    //                     child[key] = value;
    //                 });
    //                 values = values.concat(childs);
    //             }else{
    //                 if (key.toLowerCase().indexOf('geohash') >= 0) {
    //                     key = 'geohash';
    //                 }
    //                 let obj = {};
    //                 obj[key] = value;
    //                 obj[newKey] = el[newKey].value;
    //                 values.push(obj);
    //             }
    //         } else if (Object.keys(el).length === 2) {
    //             // geohash grid
    //             values.push({
    //                 geohash: value,
    //                 count: el.doc_count
    //             });
    //         }
    //     });
    //     return values.map((el) => {
    //         return CSVSerializer.formatAlias(el, parsed);
    //     });
    // }

    static formatAlias(el, parsed) {
        if (parsed && el) {
            for (let i = 0, length = parsed.select.length; i < length; i++) {
                const sel = parsed.select[i];
                if (sel.alias) {
                    if (sel.type === 'literal') {
                        el[sel.alias] = el[sel.value];
                        delete el[sel.value];
                    } else if (sel.type === 'function') {
                        const name = Json2sql.parseFunction(sel);
                        if (el[name]) {
                            el[sel.alias] = el[name];
                            delete el[name];
                        }
                    }

                }
            }
        }
        return el;
    }

    static serialize(data, parsed, id) {
        let ast = null;
        logger.debug('Resultado', data);
        if (data && data.length > 0) {

            if (data[0].aggregations) {

                let keys = Object.keys(data[0].aggregations);
                let attributes = {};
                if (!data[0].aggregations[keys[0]].buckets) {
                    for (let i = 0, length = keys.length; i < length; i++) {
                        attributes[keys[i]] = data[0].aggregations[keys[i]].value;
                    }
                    return {
                        data: [attributes]
                    };
                } else {
                    logger.debug(' data[0].aggregations[keys[0]]',  data[0].aggregations[keys[0]]);
                    const values = CSVSerializer.serializeBucket(keys[0], data[0].aggregations[keys[0]].buckets);
                    const list = values.map((el) => {
                        return CSVSerializer.formatAlias(el, parsed);
                    });
                    return {
                        data: list
                    };
                }

            } else if (data[0].hits && data[0].hits.hits && data[0].hits.hits.length > 0) {
                return {
                    data: data[0].hits.hits.map((el) => CSVSerializer.formatAlias(Object.assign(el._source, {
                        _id: el._id
                    }), parsed)) // jshint ignore:line
                };
            }
        }
        return {
            data: []
        };

    }
}

module.exports = CSVSerializer;

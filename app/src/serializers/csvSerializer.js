
const Json2sql = require('sql2json').json2sql;
const GeoJSON = require('geojson');


class CSVSerializer {

    static serializeBucket(key, buckets) {
        let list = [];
        let alias = key;
        if (key.indexOf('geohash') >= 0) {
            alias = 'geohash';
        }
        alias = alias.replace('.keyword', '');
        for (let i = 0, length = buckets.length; i < length; i++) {
            let keys = Object.keys(buckets[i]).filter((el) => el !== 'doc_count' && el !== 'key');
            if (keys.length === 1 && buckets[i][keys[0]].buckets) {
                const partialList = CSVSerializer.serializeBucket(keys[0], buckets[i][keys[0]].buckets);
                for (let j = 0, length = partialList.length; j < length; j++) {
                    partialList[j][alias] = buckets[i].key;
                }
                list = list.concat(partialList);
            } else {
                let obj = {
                    [alias]: buckets[i].key
                };
                for (let j = 0, lengthSublist = keys.length; j < lengthSublist; j++) {
                    if (buckets[i][keys[j]].value) {
                        obj[keys[j]] = buckets[i][keys[j]].value;
                    }
                }
                list.push(obj);
            }
        }
        return list;
    }

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

    static serialize(data, parsed, id, format = 'json') {
        if (data && data.length > 0) {

            if (data[0].aggregations) {

                const keys = Object.keys(data[0].aggregations);
                const attributes = {};
                if (!data[0].aggregations[keys[0]].buckets) {
                    for (let i = 0, length = keys.length; i < length; i++) {
                        attributes[keys[i]] = data[0].aggregations[keys[i]].value;
                    }
                    return {
                        data: [attributes]
                    };
                }
                const values = CSVSerializer.serializeBucket(keys[0], data[0].aggregations[keys[0]].buckets);
                const list = values.map((el) => {
                    return CSVSerializer.formatAlias(el, parsed);
                });
                return {
                    data: list
                };

            } else if (data[0].hits && data[0].hits.hits && data[0].hits.hits.length > 0) {

                return {
                    data: data[0].hits.hits.map((el) => {
                        const formatted = CSVSerializer.formatAlias(Object.assign(el._source, {
                            _id: el._id
                        }), parsed);
                        if (format === 'geojson') {
                            return GeoJSON.parse(formatted, { exclude: ['the_geom'], GeoJSON: 'the_geom' });
                        }
                        return formatted;
                    }) // jshint ignore:line
                };
            }
        }
        return {
            data: []
        };

    }

}

module.exports = CSVSerializer;

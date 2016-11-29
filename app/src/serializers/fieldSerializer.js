'use strict';

var logger = require('logger');

class FieldSerializer {

    static searchProperties(el) {
        if (!el.properties && Object.keys(el).length === 1) {
            return FieldSerializer.searchProperties(el[Object.keys(el)[0]]);
        }
        return el.properties;
    }

    static serialize(data, tableName, id) {
        if (data && data.length > 0) {
            // return {
            //     data: {
            //         id: id,
            //         type: 'csv',
            //         attributes: {
            //             tableName: tableName,
            //             fields: FieldSerializer.convertToArray(FieldSerializer.searchProperties(data[0]))
            //         }
            //     }
            // };
            return {
                tableName: tableName,
                fields: FieldSerializer.searchProperties(data[0])
            };
        }
    }
}

module.exports = FieldSerializer;

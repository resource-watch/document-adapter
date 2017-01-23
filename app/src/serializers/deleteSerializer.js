'use strict';
var JSONAPISerializer = require('jsonapi-serializer').Serializer;
var deleteSerializer = new JSONAPISerializer('query', {
    attributes: ['deleted'],
    typeForAttribute: function (attribute, record) {
        return attribute;
    },
    keyForAttribute: 'camelCase'
});


class DeleteSerializer {
    static serialize(data){
        return deleteSerializer.serialize(data);
    }
}

module.exports = DeleteSerializer;

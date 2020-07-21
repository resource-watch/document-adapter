class FieldSerializer {

    static serialize(data, tableName) {
        if (!data) {
            return {};
        }

        return {
            tableName,
            fields: data
        };
    }

}

module.exports = FieldSerializer;

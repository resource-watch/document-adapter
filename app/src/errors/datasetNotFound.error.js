class DatasetNotFound extends Error {

    constructor(message) {
        super(message);
        this.name = 'DatasetNotFound';
        this.message = message;
        this.status = 404;
    }

}

module.exports = DatasetNotFound;

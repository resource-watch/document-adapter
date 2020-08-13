class InvalidFormat extends Error {

    constructor(messages) {
        super(InvalidFormat.getMessages(messages));
        this.status = 400;
    }

    static getMessages(messagesObject) {
        let messages = '- ';
        messagesObject.forEach((message) => {
            if (typeof message === 'object' && message !== null) {
                messages += `${Object.keys(message)[0]}: ${message[Object.keys(message)[0]]} - `;
            } else {
                messages += `${message} - `;
            }
        });
        return messages;
    }

}

module.exports = InvalidFormat;

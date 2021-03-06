const config = require('config');
const logger = require('logger');
const Koa = require('koa');
const compress = require('koa-compress');
const bodyParser = require('koa-bodyparser');
const koaLogger = require('koa-logger');
const loader = require('loader');
const koaValidate = require('koa-validate');
const ErrorSerializer = require('serializers/errorSerializer');
const { RWAPIMicroservice } = require('rw-api-microservice-node');
const koaSimpleHealthCheck = require('koa-simple-healthcheck');

// const nock = require('nock');
// nock.recorder.rec();

const app = new Koa();

app.use(compress());
// if environment is dev then load koa-logger
if (process.env.NODE_ENV === 'dev') {
    app.use(koaLogger());
}

app.use(bodyParser({
    jsonLimit: '50mb'
}));
app.use(koaSimpleHealthCheck());

// catch errors and send in jsonapi standard. Always return vnd.api+json
app.use(async (ctx, next) => {
    try {
        await next();
    } catch (err) {
        ctx.status = err.status || err.statusCode || 500;
        if (ctx.status >= 500) {
            logger.error(err);
        } else {
            logger.info(err);
        }

        ctx.body = ErrorSerializer.serializeError(ctx.status, err.message);
        if (process.env.NODE_ENV === 'prod' && ctx.status === 500) {
            ctx.body = 'Unexpected error';
        }
    }
    ctx.response.type = 'application/vnd.api+json';
});

// load custom validator
koaValidate(app);

app.use(RWAPIMicroservice.bootstrap({
    logger,
    gatewayURL: process.env.GATEWAY_URL,
    microserviceToken: process.env.MICROSERVICE_TOKEN,
    fastlyEnabled: process.env.FASTLY_ENABLED,
    fastlyServiceId: process.env.FASTLY_SERVICEID,
    fastlyAPIKey: process.env.FASTLY_APIKEY
}));

// load routes
loader.loadRoutes(app);

// get port of environment, if not exist obtain of the config.
// In production environment, the port must be declared in environment variable
const port = process.env.PORT || config.get('service.port');

const server = app.listen(process.env.PORT);

logger.info(`Server started in port:${port}`);

module.exports = server;

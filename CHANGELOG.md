## 31/05/2021

- Update `rw-api-microservice-node` to add CORS support.

## 30/03/2021

- Modify API HTTP verbs to match public API method signature.

## 12/02/2021

- Remove dependency on CT's `authenticated` functionality

## 10/02/2021

- Add error handler and message for the "too_many_buckets" error when using too many "group by" values.

## 14/12/2020

- Replace CT integration library

# 2.1.0

## 17/11/2020

- Remove dependency on CTs filter functionality
- Fix issue with number of concurrent scroll usages causing random failures in queries.
- Fix issue where aggregated column alias would be forced to lower case.
- Make ES query errors more visible to end users.
- Allow ADMIN users to reindex datasets with overwrite=false
- Modify tests to run with Opendistro for ES instance.
- Add username and password support for Elasticsearch connection
- Remove query v2 endpoint

# 2.0.0

## 01/09/2020

- Migrate to Elasticsearch 7.x
- Clarify error messages due to malformed queries

# v1.1.1

## 13/08/2020

- Refactor `/:dataset/data-overwrite` endpoint to remove dependency on injected dataset details.
- Refactor endpoints to remove dependency on injected dataset details.
- Remove `filter` values for routes exclusive to this microservice.
- Remove `/data/:dataset/:id` stub route.
- Refactor tests for faster execution.
- Add validation for `format` query param on download endpoint.
- Fix issue where attempting to download a csv of a query resulting in an empty result would cause an error.

# v1.1.0

## 09/04/2020

- Add node affinity to kubernetes configuration.

# v1.0.1

## 18/03/2020
- Fix issue where queries with a `order by` clause would fail.

# v1.0.0

## 05/12/2019
- Fix issue with loading fields data.

## 14/11/2019
- Refactor usage of JSONAPI deserializer
- Updated ElasticSearch integration lib
- Replaced generators with async/await

## 12/11/2019
- Add support for dataset overwrite using multiple files in parallel.
- Update node version to 12.
- Replace npm with yarn.
- Add liveliness and readiness probes.
- Add resource quota definition for kubernetes.

# Previous
- Malformed queries now return a 400 HTTP error code, instead of 500

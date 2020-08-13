## 13/08/2020

- Refactor endpoints to remove dependency on injected dataset details.
- Remove `filter` values for routes exclusive to this microservice.
- Remove `/data/:dataset/:id` stub route.
- Refactor tests for faster execution.

## 19/05/2020

- Add validation for `format` query param on download endpoint.

## 23/02/2020

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

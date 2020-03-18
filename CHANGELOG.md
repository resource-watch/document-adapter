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

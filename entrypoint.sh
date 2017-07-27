#!/bin/bash
set -e

case "$1" in
    develop)
        echo "Running Development Server"
        exec grunt --gruntfile app/Gruntfile.js | bunyan
        ;;
    developWorker)
        echo "Running Worker"
        exec  node app/worker | bunyan
        ;;
    startDev)
        echo "Running Start Dev"
        exec node app/index
        ;;
    test)
        echo "Running Test"
        exec grunt --gruntfile app/Gruntfile.js test
        ;;
    start)
        echo "Running Start"
        exec npm start
        ;;
    startWorker)
        echo "Running Worker start"
        export NODE_PATH=app/src
        exec node app/worker.js
        ;;
    *)
        exec "$@"
esac

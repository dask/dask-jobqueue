#!/bin/bash

# start sge
sudo service gridengine-exec restart

sleep 4

sudo service gridengine-exec restart

if [ "$TRAVIS_PYTHON_VERSION" == "2.7" ]; then
    python -m SimpleHTTPServer 8888
else
    python -m http.server 8888
fi

#!/bin/bash

if [ ! -f "requests-2.9.1.tar.gz" ]
then
    echo "requests binary file found"
    curl -O  https://pypi.python.org/packages/source/r/requests/requests-2.9.1.tar.gz || echo "download requests failed"
fi
tar -zxvf requests-2.9.1.tar.gz
cd requests-2.9.1
python setup.py install
cd ..

if [ ! -f "APScheduler-3.0.5.tar.gz" ]
then
    echo "APScheduler binary file found"
    curl -O  https://pypi.python.org/packages/source/A/APScheduler/APScheduler-3.0.5.tar.gz || echo "download APScheduler failed"
fi
tar -zxvf APScheduler-3.0.5.tar.gz
cd APScheduler-3.0.5
python setup.py install
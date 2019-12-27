#!/usr/bin/env bash

kubectl create secret -n kylin generic kylin-keytab --from-file=conf/kylin.keytab
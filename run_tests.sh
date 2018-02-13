#!/bin/bash
set -euo pipefail
IFS=$'\n\t'
if [ ! -d 'env' ]; then
    echo "setting up virtualenv"
    python3 -m virtualenv env
fi
if [ -e requirements-base.txt ]; then
    env/bin/pip -q install -r requirements-base.txt
fi
if [ -e requirements-development.txt ]; then
    env/bin/pip -q install -r requirements-development.txt
fi
if [ -e requirements-testing.txt ]; then
    env/bin/pip -q install -r requirements-testing.txt
fi
env/bin/python -m tornado.testing discover -s toshieth/test

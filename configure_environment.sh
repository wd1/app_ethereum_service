#!/bin/bash
if [ ! -z ${ENV_DIR+x} ]; then
    STAGE=$(cat "$ENV_DIR/STAGE")
fi
echo "BUILDING requirements.txt WITH STAGE=$STAGE"
cat requirements-base.txt > requirements.txt
if [[ $STAGE == "production" ]]; then
    cat requirements-production.txt >> requirements.txt
else
    cat requirements-development.txt >> requirements.txt
fi
cat requirements.txt

#!/bin/bash

set -e

if [[ "$TRAVIS_SECURE_ENV_VARS" == "false" ]]; then
  echo "skip docker upload, TRAVIS_SECURE_ENV_VARS=$TRAVIS_SECURE_ENV_VARS"
  exit 0
fi

BRANCH=${BRANCH:?'missing BRANCH env var'}

source ~/.nvm/nvm.sh
nvm use 6

export DOCKER_NAMESPACE=jaegertracing
make docker

for component in agent 
do
  export REPO="jaegertracing/jaeger-${component}"
  bash ./scripts/travis/upload-to-docker.sh
done

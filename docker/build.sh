#!/bin/bash

# Build and tag Python 3.7 images
docker build --build-arg PYTHON_VERSION=3.7 -t pyalgotrade:0.20 .
docker tag pyalgotrade:0.20 pyalgotrade:0.20-py37
docker tag gansaihua/pyalgotrade:0.20 pyalgotrade:0.20-py37

# Push images
docker login --username=gansaihua

# docker push gbecedillas/pyalgotrade:0.20
docker push gansaihua/pyalgotrade:0.20-py37

# docker rmi $(docker images --quiet --filter "dangling=true")

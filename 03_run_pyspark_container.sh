#!/bin/bash

dir_project_root="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
docker run -it --rm --name learn_pyspark -v "${dir_project_root}":/home/jovyan -v "${HOME}/.aws":/home/jovyan/.aws -p 8888:8888 jupyter/pyspark-notebook:aarch64-spark-3.3.0

#!/bin/sh

function run(){

    SOURCE="${BASH_SOURCE[0]}"

    while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
      DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
      SOURCE="$(readlink "$SOURCE")"
      [[ ${SOURCE} != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
    done
    DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"

    DATA=${DIR}/../data

    if [ ! -d "${DATA}" ]; then
        # Control will enter here if $DIRECTORY exists.
        mkdir ${DATA}
        chmod 777 ${DATA} -R
    fi
    echo ${DATA}

    S3COPY=/volumes/neo/neumann/s3copy
    if [ ! -d "${S3COPY}" ]; then
        mkdir ${S3COPY}
        chmod 777 ${S3COPY} -R
    fi
    echo ${S3COPY}

    docker rm -f neumann
    docker run -it -d -P -v ${DATA}:/app/data -v ${S3COPY}:/app/s3copy --name neumann predictry/neumann
}

run
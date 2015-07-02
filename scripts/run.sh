#!/bin/sh

function run(){

    docker run -it -d -P 80 -P 8082 -v data:/app/data --name neumann predictry/neumann
}

run
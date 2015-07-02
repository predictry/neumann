#!/bin/sh

function run(){

    docker run -it -d -P -v  $PWD/data:/app/data --name neumann predictry/neumann
}

run
#!/usr/bin/env bash

USAGE="-e Usage: build-images.sh\n\t
        [server|interpreter_base|interpreter {interpreter_name}]"

bin=$(dirname "${BASH_SOURCE-$0}")
bin=$(cd "${bin}">/dev/null; pwd)

. "${bin}/common.sh"

VERSION=$(getZeppelinVersion)

function copy_zeppelin_dist() {
    # copy zeppelin-distribution if it doesn't exist
    if [[ -d ${ZEPPELIN_HOME}/zeppelin-distribution/target/zeppelin-${VERSION}/zeppelin-${VERSION} ]]; then
        echo "Copying zeppelin dist to zeppelin-${1}"
        cp -r ${ZEPPELIN_HOME}/zeppelin-distribution/target/zeppelin-${VERSION}/zeppelin-${VERSION} ${ZEPPELIN_HOME}/scripts/docker/zeppelin-${1}
    fi
}


function build_zeppelin_server_image() {
    copy_zeppelin_dist server
    echo "Building zeppelin server image"
    docker build --build-arg version=${VERSION} -t ${REPO}/zeppelin-server:${VERSION} ${ZEPPELIN_HOME}/scripts/docker/zeppelin-server
    echo "Pushing zeppelin server image"
    docker push ${REPO}/zeppelin-server:${VERSION}
}


function build_zeppelin_interpreter_base_image() {
    copy_zeppelin_dist interpreter
    echo "Building interpreter base image"
    docker build --build-arg version=${VERSION} -t ${REPO}/zeppelin-interpreter-base:${VERSION} -f ${ZEPPELIN_HOME}/scripts/docker/zeppelin-interpreter/Dockerfile_base \
        ${ZEPPELIN_HOME}/scripts/docker/zeppelin-interpreter
    echo "Pushing zeppelin interpreter base image"
    docker push ${REPO}/zeppelin-interpreter-base:${VERSION}
}

# build the specific zeppelin-interpreter image
function build_zeppelin_interpreter_image() {
    interpreter_name=$1
    copy_zeppelin_dist interpreter

    dockerfile="${ZEPPELIN_HOME}/scripts/docker/zeppelin-interpreter/Dockerfile"

    if [[ -f "${dockerfile}_${interpreter_name}" ]]; then
        dockerfile="${dockerfile}_${interpreter_name}"
    fi

    echo "Building interpreter ${interpreter_name} image via docker file: ${dockerfile}"
    docker build -t ${REPO}/zeppelin-interpreter-${interpreter_name}:${VERSION} \
      -f ${dockerfile} \
      --build-arg interpreter_name=${interpreter_name} --build-arg repo=${REPO} --build-arg version=${VERSION} \
      ${ZEPPELIN_HOME}/scripts/docker/zeppelin-interpreter
    echo "Pushing interpreter ${interpreter_name} image"
    docker push ${REPO}/zeppelin-interpreter-${interpreter_name}:${VERSION}
}

REPO=apache

while getopts ":r:" opt; do
  case ${opt} in
    r)
      REPO=$OPTARG
      ;;
   \?)
     echo $opt
     echo "Invalid Option: -$OPTARG" 1>&2
     exit 1
     ;;
  esac
done

shift $((OPTIND -1))
subcommand=$1;

case "$subcommand" in
  server)
    build_zeppelin_server_image
    ;;
  interpreter_base)
    build_zeppelin_interpreter_base_image
    ;;
  interpreter)
    build_zeppelin_interpreter_image $2
    ;;
  *)
    echo ${USAGE}
esac

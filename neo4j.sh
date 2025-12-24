#!/usr/bin/env bash
set -Eeuo pipefail

name="neo4j-db"
image="neo4j:5.14"

log() { echo "[neo4j] $*" >&2; }

container_exists() {
    docker ps -a --format '{{.Names}}' | grep -qx "$name"
}

container_running() {
    docker inspect -f '{{.State.Running}}' "$name" 2>/dev/null | grep -qx true
}

run_container() {
    docker run \
        --name "$name" \
        -p7474:7474 \
        -p7687:7687 \
        -e NEO4J_AUTH=neo4j/testTEST12345678 \
        -v neo4j_data:/data \
        "$image"
}

start_container() {
    docker start "$name"
}

main() {
    if container_running; then
        log "Already running"
        exit 0
    fi

    if container_exists; then
        log "Starting existing container"
        start_container
    else
        log "Creating and running new container"
        run_container
    fi

    log "OK"
}

main

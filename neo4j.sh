#!/bin/bash
set -e

docker start neo4j-db 2>/dev/null || \
docker run --name neo4j-db \
  -p7474:7474 -p7687:7687 \
  -e NEO4J_AUTH=neo4j/testTEST12345678 \
  -v neo4j_data:/data \
  neo4j:5.14

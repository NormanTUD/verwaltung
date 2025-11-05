#!/bin/bash
set -e

#echo "Starting neo4j"
#docker start neo4j-db 2>/dev/null
#echo "Starting neo4j: $?"

echo "Running neo4j"
docker run --name neo4j-db -p7474:7474 -p7687:7687 -e NEO4J_AUTH=neo4j/testTEST12345678 -v neo4j_data:/data neo4j:5.14
echo "Running neo4j: $?"

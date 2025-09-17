#!/bin/bash

docker run --name neo4j-db -p7474:7474 -p7687:7687 -e NEO4J_AUTH=neo4j/test1234 -v neo4j_data:/data neo4j:5.12

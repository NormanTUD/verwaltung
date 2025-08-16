#!/bin/bash

docker pull neo4j:5

docker run -d \                                                           
  --name my-neo4j \
  -p 7474:7474 \
  -p 7687:7687 \
  -e NEO4J_dbms_security_auth__minimum__password__length=6 -e NEO4J_AUTH=neo4j/test123 \
  neo4j:5


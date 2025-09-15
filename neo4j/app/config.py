import os

class Config:
    SECRET_KEY = os.getenv("SECRET_KEY", "dev-secret")
    NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
    NEO4J_PASS = os.getenv("NEO4J_PASS", "test1234")


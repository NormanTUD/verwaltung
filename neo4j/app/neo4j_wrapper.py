from neo4j import GraphDatabase, basic_auth
from neo4j.exceptions import Neo4jError

class Neo4jWrapper:
    def __init__(self, uri: str, user: str, password: str):
        if not uri or not user or not password:
            raise ValueError("Neo4j URI, user and password must be provided")
        self._driver = GraphDatabase.driver(uri, auth=basic_auth(user, password))

    def close(self):
        try:
            self._driver.close()
        except Exception:
            pass

    def run_cypher(self, cypher: str, parameters: dict = None, return_results: bool = True):
        parameters = parameters or {}
        with self._driver.session() as session:
            try:
                result = session.run(cypher, **parameters)
                if return_results:
                    records = [r.data() for r in result]
                    return records
                return None
            except Neo4jError as e:
                raise

    def execute_write(self, fn, *args, **kwargs):
        with self._driver.session() as session:
            return session.execute_write(lambda tx: fn(tx, *args, **kwargs))

    def execute_read(self, fn, *args, **kwargs):
        with self._driver.session() as session:
            return session.execute_read(lambda tx: fn(tx, *args, **kwargs))

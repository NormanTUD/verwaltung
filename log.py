import logging
logging.basicConfig(level=logging.ERROR)
mod_loggers = {logging.getLogger("py2neo"),
               logging.getLogger("neo4j"),
               logging.getLogger("werkzeug"),
               logging.getLogger("faker")
               }
for log in mod_loggers:
    log.setLevel(logging.WARNING)

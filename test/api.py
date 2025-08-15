from fastapi import FastAPI
from arango_client import get_entities, create_entity, link_entities
from arango import ArangoClient
from pydantic import BaseModel

app = FastAPI()

class EntityModel(BaseModel):
    name: str
    attributes: dict

class LinkModel(BaseModel):
    from_id: str
    to_id: str
    label: str

@app.get("/entities")
def list_entities():
    return get_entities()

@app.post("/entities")
def add_entity(entity: EntityModel):
    return create_entity(entity.dict())

@app.post("/link")
def add_link(link: LinkModel):
    return link_entities(link.from_id, link.to_id, link.label)

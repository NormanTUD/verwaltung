from fastapi import FastAPI, HTTPException, Request, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
import os
import csv
import io

# MongoDB Connection
client = MongoClient(os.getenv('MONGO_URL', 'mongodb://mongodb:27017/'))
db = client.get_database('dynamic_db')

app = FastAPI(title="Dynamic Data API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class ItemUpdate(BaseModel):
    collection: str
    item_id: str
    data: Dict[str, Any]

class NewColumn(BaseModel):
    collection_name: str
    column_name: str

@app.get("/collections")
async def get_collections():
    """List all collections in the database."""
    return {"collections": db.list_collection_names()}

@app.get("/data/{collection_name}")
async def get_collection_data(collection_name: str):
    """Get all documents from a specific collection."""
    if collection_name not in db.list_collection_names():
        raise HTTPException(status_code=404, detail="Collection not found")
    
    collection = db[collection_name]
    items = list(collection.find({}))
    
    for item in items:
        item['_id'] = str(item['_id'])
    
    return {"data": items}

@app.post("/update_item")
async def update_item(item: ItemUpdate):
    """Update a specific item in a collection."""
    from bson.objectid import ObjectId
    try:
        collection = db[item.collection]
        result = collection.update_one(
            {"_id": ObjectId(item.item_id)},
            {"$set": item.data}
        )
        if result.modified_count == 0:
            return {"message": "No changes made."}
        return {"message": "Item updated successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/add_column")
async def add_column(column: NewColumn):
    """Add a new column dynamically to a collection."""
    collection = db[column.collection_name]
    collection.update_many(
        {},
        {"$set": {column.column_name: None}}
    )
    return {"message": f"Spalte '{column.column_name}' zu {column.collection_name} hinzugefügt."}

@app.post("/import_csv/{collection_name}")
async def import_csv(collection_name: str, file: UploadFile = File(...), link_to: Optional[str] = Form(None)):
    """Handle CSV import."""
    try:
        content = await file.read()
        csv_file = io.StringIO(content.decode('utf-8'))
        reader = csv.DictReader(csv_file)
        
        collection = db[collection_name]
        
        for row in reader:
            doc = {}
            for key, value in row.items():
                if link_to and key.lower() == link_to.lower():
                    # Create reference to another collection by name
                    linked_doc = db[link_to].find_one({ "name": value })
                    if linked_doc:
                        doc[link_to.lower()] = linked_doc['_id']
                    else:
                        doc[link_to.lower()] = value # Store value if not found
                else:
                    doc[key] = value
            collection.insert_one(doc)
            
        return {"message": f"Daten erfolgreich in '{collection_name}' importiert."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

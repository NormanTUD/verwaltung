async function loadEntities() {
    let resp = await fetch('http://localhost:8000/entities');
    let data = await resp.json();
    console.log("Loaded entities:", data);
    // Hier dynamisch Tabellen rendern, Drag & Drop aktivieren
}

async function addColumn(entityId, columnName, refEntityId=null) {
    if(refEntityId){
        await fetch('http://localhost:8000/link', {
            method:'POST',
            headers:{'Content-Type':'application/json'},
            body: JSON.stringify({from_id: entityId, to_id: refEntityId, label: columnName})
        });
    } else {
        // normale Spalte
    }
}

window.onload = loadEntities;

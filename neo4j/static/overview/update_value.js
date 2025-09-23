function updateValue(element) {
    console.groupCollapsed("updateValue triggered");
    try {
        if (!element) {
            warning("Abbruch: Kein Element übergeben");
            console.groupEnd();
            return;
        }

        const newValue = element.value;
        if (element.originalValue === newValue) {
            console.log("Wert unverändert, keine Aktion nötig");
            console.groupEnd();
            return;
        }
        element.originalValue = newValue;

        const propertyName = element.getAttribute('data-property');
        if (!propertyName) {
            warning("data-property fehlt");
            console.groupEnd();
            return;
        }

        const dataIdAttr = element.getAttribute('data-id');
        if (dataIdAttr && dataIdAttr !== "null") {
            updateExistingNodes(dataIdAttr, propertyName, newValue);
        } else {
            createNewNode(element, propertyName, newValue);
        }

    } catch (err) {
        error('updateValue exception: ' + err);
    }
    console.groupEnd();
}

// === Update bestehender Node(s) ===
function updateExistingNodes(dataIdAttr, property, value) {
    console.group("Update vorhandene Nodes");
    const ids = dataIdAttr.split(',').map(s => Number(s.trim()));
    console.log("IDs:", ids);

    fetch('/api/update_nodes', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ids, property, value })
    })
        .then(r => r.json())
        .then(d => {
            console.log("update_nodes response:", d);
            if (d.status === "success") success(d.message);
            else error(d.message);
        })
        .catch(err => error("update_nodes fetch error: " + err))
        .finally(() => console.groupEnd());
}

// === Node erstellen, falls noch nicht vorhanden ===
function createNewNode(element, property, value) {
    const tr = element.closest('tr');
    if (!tr) {
        warning("Kein tr-Element gefunden");
        return;
    }

    const connectedIds = collectConnectedIds(tr);
    const uniqueRelations = collectUniqueRelations(element);

    if (uniqueRelations.length === 0) {
        console.log("Keine Relation, Node ohne Relation erstellen");
        createNodeRequest(element, property, value, connectedIds, null);
    } else if (uniqueRelations.length === 1) {
        console.log("Nur eine Relation, Node direkt erstellen");
        createNodeRequest(element, property, value, connectedIds, uniqueRelations[0]);
    } else {
        console.log("Mehrere Relationen, Modal anzeigen");
        showRelationModal(uniqueRelations, relType => {
            createNodeRequest(element, property, value, connectedIds, relType);
        });
    }
}

// === Connected IDs für GESCHRIEBENVON sammeln ===
function collectConnectedIds(tr) {
    console.group("Connected IDs sammeln");
    const otherInputs = Array.from(tr.querySelectorAll('input[data-id][data-relation]'));
    const connectedIds = otherInputs.map(inp => {
        try {
            const relations = inp.getAttribute('data-relation')?.split(',') || [];
            if (relations.includes('GESCHRIEBENVON')) {
                const id = Number(inp.getAttribute('data-id'));
                console.log("GESCHRIEBENVON ID gefunden:", id);
                return id;
            }
        } catch (err) {
            error("Fehler beim Parsen der data-relation: " + err);
        }
        return null;
    }).filter(id => id !== null && !isNaN(id));
    console.log("Connected IDs:", connectedIds);
    console.groupEnd();
    return connectedIds;
}

// === Alle Relations in derselben Spalte sammeln ===
function collectUniqueRelations(element) {
    console.group("Relationen sammeln");
    const tr = element.closest('tr');
    const tdIndex = Array.from(tr.children).indexOf(element.parentElement);
    const table = element.closest('table');
    const relationSet = new Set();

    table.querySelectorAll('tbody tr').forEach((row, rowIndex) => {
        const td = row.children[tdIndex];
        const relData = td?.getAttribute('data-relations');
        if (relData) {
            try {
                const parsed = JSON.parse(decodeURIComponent(relData));
                parsed.forEach(r => {
                    relationSet.add(r.relation);
                    console.log(`Row ${rowIndex}: Relation gefunden:`, r.relation);
                });
            } catch (err) {
                warning(`Row ${rowIndex}: JSON parse error: ${err}`);
            }
        }
    });

    const uniqueRelations = Array.from(relationSet);
    console.log("Unique Relations:", uniqueRelations);
    console.groupEnd();
    return uniqueRelations;
}

// === Node via API erstellen ===
function createNodeRequest(element, property, value, connectedIds, relType) {
    console.group("createNodeRequest");
    console.log("Property:", property, "Value:", value, "ConnectTo:", connectedIds, "Relation:", relType);

    fetch('/api/create_node', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
            property: property,
            value: value,
            connectTo: connectedIds,
            relation: relType ? { relation: relType, targetLabel: "Buch" } : { targetLabel: "Buch" }
        })
    })
        .then(r => r.json())
        .then(data => {
            console.log("create_node response:", data);
            if (data.status === 'success') {
                element.setAttribute('data-id', data.newNodeId);
                success("Node erfolgreich erstellt, ID: " + data.newNodeId);
            } else {
                error("create_node fehlgeschlagen: " + data.message);
            }
        })
        .catch(err => error("create_node fetch error: " + err))
        .finally(() => console.groupEnd());
}

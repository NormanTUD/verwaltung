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
// Hier ist die wirklich dynamische createNewNode-Funktion, hardcode-frei
function createNewNode(element, property, value) {
    console.group("createNewNode: Start");
    const tr = element.closest('tr');
    if (!tr) {
        warning("Abbruch: Kein tr-Element gefunden");
        console.groupEnd();
        return;
    }
    
    // Hole das Node-Label aus der Spaltenüberschrift
    const tdIndex = Array.from(tr.children).indexOf(element.parentElement);
    const tableHeader = tr.closest('table').querySelector('thead tr');
    const headerCell = tableHeader.children[tdIndex];
    const headerText = headerCell.textContent.trim();
    const parts = headerText.split(':');
    const nodeLabel = parts[0];
    
    // Hole alle Relationen der Zeile, um die verbundene ID zu finden
    const td = element.parentElement;
    const relDataAttr = td.getAttribute('data-relations');
    let allRowRelations = [];
    if (relDataAttr) {
        try {
            allRowRelations = JSON.parse(decodeURIComponent(relDataAttr));
        } catch (err) {
            error("Fehler beim Parsen der Relationen: " + err);
            console.groupEnd();
            return;
        }
    }
    
    let connectedData = [];
    if (allRowRelations.length > 0) {
        // Die verbundene ID ist die 'fromId' des ersten Nodes in der Zeile
        const connectId = allRowRelations[0].fromId;
        if (connectId) {
            connectedData.push({ id: connectId }); // Sende nur die ID
        }
    }

    // Rufe createNodeRequest ohne relType auf
    createNodeRequest(element, property, value, connectedData, nodeLabel);

    console.groupEnd();
}

// === Connected IDs dynamisch aus allen TDs sammeln ===
function collectAllConnectedIds(tr) {
    const connected = new Map();
    tr.querySelectorAll('td[data-relations]').forEach(td => {
        try {
            const relData = td.getAttribute('data-relations');
            if (!relData) return;
            const parsed = JSON.parse(decodeURIComponent(relData));
            parsed.forEach(rel => {
                if (rel.fromId) {
                    if (!connected.has(rel.fromId)) {
                        connected.set(rel.fromId, { id: rel.fromId, relation: rel.relation, isFrom: true, isTo: false });
                    }
                }
                if (rel.toId) {
                    if (!connected.has(rel.toId)) {
                        connected.set(rel.toId, { id: rel.toId, relation: rel.relation, isFrom: false, isTo: true });
                    }
                }
            });
        } catch (err) {
            console.warn("Error parsing relations:", err);
        }
    });
    return Array.from(connected.values());
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

// === Node via API erstellen, komplett dynamisch ===
// Hier ist die angepasste createNodeRequest Funktion
function createNodeRequest(element, property, value, connectTo, nodeLabel) {
    console.group("createNodeRequest: Start");

    const props = {};
    props[property] = value;
    
    const requestBody = { props, node_label: nodeLabel };

    if (connectTo && Array.isArray(connectTo) && connectTo.length > 0) {
        requestBody.connectTo = connectTo;
    } else {
        console.log("DEBUG: Keine gültigen Verbindungsdaten gefunden. Sende Request ohne `connectTo`.");
        requestBody.connectTo = [];
    }
    
    console.log("DEBUG: Sende Request mit Body:", JSON.stringify(requestBody, null, 2));

    fetch('/api/create_node', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(requestBody)
    })
    .then(r => {
        console.log("DEBUG: Empfange Response mit Status:", r.status);
        return r.json();
    })
    .then(data => {
        console.log("DEBUG: Response-Daten:", data);
        if (data.status === 'success') {
            element.setAttribute('data-id', data.newNodeId);
            success("Node erfolgreich erstellt, ID: " + data.newNodeId);
        } else {
            error("create_node fehlgeschlagen: " + data.message);
        }
    })
    .catch(err => {
        console.error("DEBUG: Fetch-Fehler:", err);
        error("create_node fetch error: " + err);
    })
    .finally(() => {
        console.groupEnd();
    });
}

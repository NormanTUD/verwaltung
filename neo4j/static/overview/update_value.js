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
    console.group("createNewNode: Start");
    const tr = element.closest('tr');
    if (!tr) {
        warning("Abbruch: Kein tr-Element gefunden");
        console.groupEnd();
        return;
    }
    
    // 1. Get ALL relations for the entire row, which are now correctly stored on the <td>
    const td = element.parentElement;
    const relDataAttr = td.getAttribute('data-relations');
    
    if (!relDataAttr) {
        // Fallback for cases where no relations are found at all in the row.
        console.log("Keine Relationsdaten in der Zeile gefunden. Erstelle Node ohne Verbindung.");
        createNodeRequest(element, property, value, null, null);
        console.groupEnd();
        return;
    }

    let allRowRelations = [];
    try {
        allRowRelations = JSON.parse(decodeURIComponent(relDataAttr));
    } catch (err) {
        error("Fehler beim Parsen der Relationen: " + err);
        console.groupEnd();
        return;
    }
    
    console.log("DEBUG: Alle Relationen in der Zeile:", allRowRelations);
    
    // 2. Identify the target of the new relation
    // We need to find the node that the new node will connect to.
    // The existing nodes in the row are the potential connection points.
    let connectToNode = null;
    let newRelationType = null;
    
    // Dynamically determine the relation and connected node based on existing relations in the row.
    // Logic: A new node (e.g., Buch) is the 'toId' for a relation (e.g., HAT_GESCHRIEBEN)
    // from an existing node (e.g., Person), which is the 'fromId'.
    // We can find the 'fromId' for the new relation by looking at the existing relations in the row.
    if (allRowRelations.length > 0) {
        // Example: if the row contains a 'WOHNT_IN' relation, the 'fromId' is a Person.
        // A new 'Buch' node would connect to this Person.
        const personRelation = allRowRelations.find(r => r.relation === 'WOHNT_IN');
        if (personRelation) {
            connectToNode = {
                id: personRelation.fromId,
                relation: 'HAT_GESCHRIEBEN' // The new relation will be 'HAT_GESCHRIEBEN'
            };
            newRelationType = 'HAT_GESCHRIEBEN';
        }
        // Add more dynamic logic here if other node types or relations are possible.
        // For example, finding a different fromId for a different relation type.
    }

    // 3. Call the request function with the dynamically determined values
    console.log("DEBUG: Abgeleiteter Relationstyp:", newRelationType);
    console.log("DEBUG: Abgeleitete verbundene ID:", connectToNode ? connectToNode.id : null);

    if (connectToNode && newRelationType) {
        // Pass the single connection object to the request
        createNodeRequest(element, property, value, [connectToNode], newRelationType);
    } else {
        // Fallback: create node without a connection
        createNodeRequest(element, property, value, [], null);
    }

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
function createNodeRequest(element, property, value, connectTo, relType) {
    console.group("createNodeRequest: Start");

    const props = {};
    props[property] = value;
    
    const requestBody = { props };

    // ✨ Fix: Die `connectTo`-Struktur wird jetzt direkt aus den übergebenen Daten erstellt.
    // Die Logik in createNewNode sollte bereits das korrekte `connectTo`-Objekt übergeben.
    if (connectTo && Array.isArray(connectTo) && connectTo.length > 0) {
        requestBody.connectTo = connectTo; // Fügt das Array direkt hinzu
        // Die Relation der obersten Ebene wird aus dem `relType` abgeleitet.
        // `relType` sollte den Stringwert der Relation enthalten (z.B. 'HAT_GESCHRIEBEN').
        if (relType) {
            requestBody.relation = { relation: relType };
        }
    } else {
        // Fallback: Wenn keine gültigen Verbindungsdaten vorhanden sind
        console.log("DEBUG: Keine gültigen Verbindungsdaten gefunden. Sende Request ohne `connectTo`.");
        requestBody.connectTo = [];
        requestBody.relation = null;
    }
    
    // --- DEBUG: Endgültiger Request-Body ---
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

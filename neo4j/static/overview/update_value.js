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
    
    // --- DEBUG: Prüfen des Elements und der umgebenden Struktur ---
    console.log("DEBUG: Element:", element);
    console.log("DEBUG: Eigenschaft:", property);
    console.log("DEBUG: Neuer Wert:", value);
    console.log("DEBUG: Übergeordnete Zeile (tr):", tr);
    
    // 1. Hole alle potenziell verbundenen IDs in der Zeile
    const allConnectedIds = collectAllConnectedIds(tr);
    console.log("DEBUG: Alle verbundenen IDs in der Zeile:", allConnectedIds);

    // 2. Bestimme den Relationstyp basierend auf der Spalte
    const tdIndex = Array.from(tr.children).indexOf(element.parentElement);
    const tableHeader = element.closest('table').querySelector('thead tr');
    const headerCell = tableHeader.children[tdIndex];
    const headerText = headerCell.textContent.trim();
    console.log("DEBUG: Spalten-Header-Text:", headerText);

    let relType = null;
    let connectToId = null;

    // Logik zur Ableitung von `relType` und `connectToId`
    if (headerText.startsWith('Buch:')) {
        // Ein Buch wird erstellt, die Relation ist wahrscheinlich 'HAT_GESCHRIEBEN'
        relType = 'HAT_GESCHRIEBEN';
        // Finde die Person, die dieses Buch geschrieben hat.
        // Die `fromId` der 'HAT_GESCHRIEBEN' Relation ist die Person.
        const personRelation = allConnectedIds.find(item => item.relation === relType);
        if (personRelation) {
            connectToId = personRelation.fromId;
        }
    } else if (headerText.startsWith('Ort:')) {
        // Ein Ort wird erstellt, die Relation ist wahrscheinlich 'WOHNT_IN'
        relType = 'WOHNT_IN';
        // Finde die Person, die an diesem Ort wohnt.
        const personRelation = allConnectedIds.find(item => item.relation === relType);
        if (personRelation) {
            connectToId = personRelation.fromId;
        }
    }
    
    // Fallback: wenn keine spezifische Relation gefunden wird, versuche es über die erste verfügbare
    if (!relType && allConnectedIds.length > 0) {
        relType = allConnectedIds[0].relation;
        connectToId = allConnectedIds[0].fromId || allConnectedIds[0].toId;
        console.log("DEBUG: Fallback-Relation verwendet.");
    }

    // --- DEBUG: Endgültige Werte vor dem Senden ---
    console.log("DEBUG: Abgeleiteter Relationstyp:", relType);
    console.log("DEBUG: Abgeleitete verbundene ID:", connectToId);

    // Aufruf der Request-Funktion mit den abgeleiteten Werten
    createNodeRequest(element, property, value, connectToId, relType);

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
function createNodeRequest(element, property, value, connectToId, relType) {
    console.group("createNodeRequest: Start");

    const props = {};
    props[property] = value;
    
    const requestBody = { props };

    if (connectToId && relType) {
        requestBody.connectTo = [{ id: connectToId, relation: relType }];
        requestBody.relation = { relation: relType };
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

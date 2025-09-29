"use strict";

const querySelection = document.getElementById('querySelection');
const resultsContainer = document.getElementById('resultsContainer');

function make_thead_from_columns(cols) {
	var thead = document.createElement('thead');
	var tr = document.createElement('tr');

	// decide if we need to prefix nodeType (when >1 distinct nodeType present)
	var types = {};
	for (var i = 0; i < cols.length; ++i) types[cols[i].nodeType] = true;
	var multi_types = Object.keys(types).length > 1;

	for (var i = 0; i < cols.length; ++i) {
		var c = cols[i];
		var th = document.createElement('th');
		th.textContent = c.nodeType + ':' + c.property;
		tr.appendChild(th);
	}

	var thR = document.createElement('th'); thR.textContent = 'Beziehungen';
	tr.appendChild(thR);

	var thPlus = document.createElement('th'); thPlus.textContent = '+';
	tr.appendChild(thPlus);

	var thAct = document.createElement('th'); thAct.textContent = 'Aktion';
	tr.appendChild(thAct);

	thead.appendChild(tr);
	return thead;
}

function make_input_td(cell, col) {
	var td = document.createElement('td');
	var input = document.createElement('input');
	input.type = 'text';
	input.value = cell ? (cell.value == null ? '' : cell.value) : '';
	if (cell && cell.nodeId != null) input.setAttribute('data-id', cell.nodeId);
	input.setAttribute('data-property', col.property || '');
	input.setAttribute('onblur', 'updateValue(this)');
	td.appendChild(input);
	return td;
}

function build_node_map_from_row(cols, cells) {
	var map = {};
	for (var i = 0; i < cols.length; ++i) {
		var c = cols[i];
		var cell = cells[i];
		if (!cell) continue;
		var id = String(cell.nodeId);
		if (!map[id]) map[id] = { props: {}, order: [] };
		var prop = c.property || ('col' + i);
		if (map[id].order.indexOf(prop) === -1) map[id].order.push(prop);
		map[id].props[prop] = cell.value;
	}
	return map;
}

function format_relations_html(rels, node_map) {
	if (!rels || !rels.length) return '';
	var parts = [];
	for (var i = 0; i < rels.length; ++i) {
		var r = rels[i];
		var from_label = node_label(String(r.fromId), node_map);
		var to_label = node_label(String(r.toId), node_map);
		parts.push(escape_html(r.relation) + ': ' + escape_html(from_label || r.fromId) + ' → ' + escape_html(to_label || r.toId));
	}
	return parts.join('<br>');
}

function node_label(id, node_map) {
	var n = node_map && node_map[id];
	if (!n) return '';
	var prefer = ['nachname','vorname','plz','straße','strasse','stadt'];
	var out = [];
	for (var i = 0; i < prefer.length; ++i) {
		if (n.props[prefer[i]]) out.push(n.props[prefer[i]]);
	}
	if (out.length) return out.join(' ');
	// fallback: all props in order
	var all = [];
	for (var j = 0; j < n.order.length; ++j) {
		var p = n.order[j];
		if (n.props[p]) all.push(n.props[p]);
	}
	return all.join(' ');
}

function first_node_id_from_row(row) {
	if (row.cells && row.cells.length) return row.cells[0].nodeId;
	return '';
}

function handle_delete_node_by_id(id, btnEl, ev) {
	if (!id) return;
	if (typeof window.deleteNode === 'function') {
		try { window.deleteNode(ev); } catch (e) { error(e); }
	} else {
		var ev = new CustomEvent('delete-node', { detail: { id: id } });
		document.dispatchEvent(ev);
	}
	// remove row from DOM for instant feedback
	var tr = btnEl && btnEl.closest && btnEl.closest('tr');
	if (tr) tr.remove();
}

function escape_html(s) {
	if (s === null || s === undefined) return '';
	return String(s).replace(/[&<>"']/g, function(m){ return ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]); });
}

function deleteNode(event) {
	console.group('deleteNode triggered');
	const button = event.target;
	const row = button.closest('tr');

	if (!row) {
		error('Kein <tr> gefunden!');
		console.groupEnd();
		return;
	}

	// Alle Inputs in dieser Zeile mit data-id sammeln
	const nodeIds = Array.from(row.querySelectorAll('input[data-id]'))
		.map(input => input.getAttribute('data-id').trim())
		.filter(id => id && id !== 'null');

	console.log('Gefundene nodeIds in der Zeile:', nodeIds);

	if (nodeIds.length === 0) {
		warning('Keine Nodes zum Löschen auf dem Server vorhanden. Entferne Zeile lokal.');
		row.remove();
		console.groupEnd();
		return;
	}

	console.log('Nodes vorhanden, sende DELETE Request:', nodeIds);

	fetch(`/api/delete_nodes?ids=${nodeIds.join(',')}`, {
		method: 'DELETE',
	})
		.then(response => {
			console.log('Fetch Response object:', response);
			if (!response.ok) {
				throw new Error(`HTTP error! status: ${response.status}`);
			}
			return response.json();
		})
		.then(data => {
			if (data.status === 'success') {
				console.log('DELETE erfolgreich, aktualisiere Tabelle...');
				fetchData();
				success(data.message);
			} else {
				warning('DELETE nicht erfolgreich:', data.message);
			}
		})
		.catch(error => {
			error('Fehler beim Löschen:', error);
		})
		.finally(() => {
			console.groupEnd();
		});
}

// === Einfaches Modal, zeigt nur Relation-Typen ===
function showRelationModal(relations, callback) {
	console.log("DEBUG: showRelationModal", relations);
	const overlay = document.createElement('div');
	overlay.style.cssText = 'position:fixed;top:0;left:0;width:100vw;height:100vh;background:rgba(0,0,0,0.5);display:flex;align-items:center;justify-content:center;z-index:9999;';

	const modal = document.createElement('div');
	modal.style.cssText = 'background:white;padding:20px;border-radius:8px;box-shadow:0 2px 10px rgba(0,0,0,0.2);min-width:300px;';

	const title = document.createElement('h3');
	title.textContent = 'Wähle die Relation für die neue Node';
	modal.appendChild(title);

	const select = document.createElement('select');
	select.style.width = '100%';
	relations.forEach(rel => {
		const option = document.createElement('option');
		option.value = rel;
		option.textContent = rel;
		select.appendChild(option);
	});
	modal.appendChild(select);

	const btnContainer = document.createElement('div');
	btnContainer.style.textAlign = 'right';
	const okBtn = document.createElement('button');
	okBtn.textContent = 'OK';
	okBtn.onclick = () => {
		console.log("DEBUG: Modal OK gedrückt, selected relation =", select.value);
		callback(select.value);
		document.body.removeChild(overlay);
	};
	btnContainer.appendChild(okBtn);

	const cancelBtn = document.createElement('button');
	cancelBtn.textContent = 'Abbrechen';
	cancelBtn.style.marginLeft = '10px';
	cancelBtn.onclick = () => {
		console.log("DEBUG: Modal Abbrechen gedrückt");
		document.body.removeChild(overlay);
	};
	btnContainer.appendChild(cancelBtn);

	modal.appendChild(btnContainer);
	overlay.appendChild(modal);
	document.body.appendChild(overlay);
}

function addColumnToNode(event) {
	let nodeIds = [];

	// IDs aus allen Inputs der Tabellenzeile sammeln
	const tr = event.target.closest('tr');
	if (tr) {
		const inputs = tr.querySelectorAll('input[data-id]');
		const idSet = new Set(); // Duplikate vermeiden

		inputs.forEach(input => {
			const ids = input.getAttribute('data-id').split(',').map(id => id.trim());
			ids.forEach(id => {
				if (id) idSet.add(Number(id));
			});
		});

		nodeIds = Array.from(idSet);
	}

	if (nodeIds.length === 0) {
		error("Keine IDs in dieser Zeile gefunden!");
		return;
	}

	// Aktive Labels aus #querySelection
	const activeLabels = Array.from(document.querySelectorAll('#querySelection input[type="checkbox"]:checked'))
		.map(cb => cb.value);

	if (activeLabels.length === 0) {
		error("Bitte mindestens einen Node-Typ im Bereich 'querySelection' auswählen!");
		return;
	}

	// Overlay erzeugen
	const overlay = document.createElement('div');
	overlay.id = 'addColumnOverlay';
	overlay.style.position = 'fixed';
	overlay.style.top = 0;
	overlay.style.left = 0;
	overlay.style.width = '100%';
	overlay.style.height = '100%';
	overlay.style.backgroundColor = 'rgba(0,0,0,0.5)';
	overlay.style.display = 'flex';
	overlay.style.alignItems = 'center';
	overlay.style.justifyContent = 'center';
	overlay.style.zIndex = 1000;

	// Modal-Inhalt
	const modal = document.createElement('div');
	modal.style.backgroundColor = '#fff';
	modal.style.padding = '20px';
	modal.style.borderRadius = '8px';
	modal.style.minWidth = '300px';
	modal.style.textAlign = 'center';
	modal.style.boxShadow = '0 0 20px rgba(0,0,0,0.3)';

	modal.innerHTML = `
	<h3>Neue Spalte hinzufügen</h3>
	<div style="margin-bottom: 10px; text-align: left;">
	    <label for="columnNameInput">Spaltenname:</label><br>
	    <input type="text" id="columnNameInput" style="width: 100%; padding: 5px;">
	</div>
	<div style="margin-bottom: 10px; text-align: left;">
	    <label for="nodeTypeSelect">Node-Typ:</label><br>
	    <select id="nodeTypeSelect" style="width: 100%; padding: 5px;">
		${activeLabels.map(label => `<option value="${label}">${label}</option>`).join('')}
	    </select>
	</div>
	<div style="margin-top: 15px;">
	    <button id="confirmAddColumnBtn" style="margin-right: 10px;">OK</button>
	    <button id="cancelAddColumnBtn">Abbrechen</button>
	</div>
    `;

	overlay.appendChild(modal);
	document.body.appendChild(overlay);

	// Cancel-Handler
	document.getElementById('cancelAddColumnBtn').addEventListener('click', () => {
		if (document.body.contains(overlay)) {
			document.body.removeChild(overlay);
		}
	});

	// OK-Handler
	document.getElementById('confirmAddColumnBtn').addEventListener('click', () => {
		const columnName = document.getElementById('columnNameInput').value.trim();
		const targetLabel = document.getElementById('nodeTypeSelect').value;

		if (!columnName) {
			error("Bitte einen Spaltennamen eingeben!");
			return;
		}

		fetch('/api/add_column', {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify({
				ids: nodeIds,
				column: columnName,
				label: targetLabel
			})
		})
			.then(response => {
				if (!response.ok) {
					return response.json().then(errorData => {
						throw new Error(errorData.message || `HTTP error! status: ${response.status}`);
					});
				}
				return response.json();
			})
			.then(data => {
				if (data.status === 'success') {
					success(data.message);
					fetchData();
				} else {
					error(data.message);
				}
			})
			.catch(error => {
				error('Fehler beim Hinzufügen der Spalte:', error);
			})
			.finally(() => {
				if (document.body.contains(overlay)) {
					document.body.removeChild(overlay);
				}
			});
	});
}

function addPropertyIfNotEmpty(inputElem) {
	const value = inputElem.value.trim();
	const ids = inputElem.getAttribute('data-id');

	if (value === "") return; // nix eintragen, wenn leer

	// Dynamischer Property-Name, z.B. aus dem Placeholder oder automatisch generiert
	const propertyName = `prop_${Date.now()}`; // eindeutiger Name, optional anpassen

	fetch('/api/update_nodes', {
		method: 'PUT',
		headers: { 'Content-Type': 'application/json' },
		body: JSON.stringify({
			ids: ids.split(','),
			property: propertyName,
			value: value
		})
	}).then(response => {
		if (!response.ok) {
			return response.json().then(errorData => {
				throw new Error(errorData.message || `HTTP error! status: ${response.status}`);
			});
		}
		return response.json();
	}).then(data => {
		console.log(data.message);
		// Optional: Header aktualisieren, damit neue Spalte sichtbar wird
		fetchData();
	}).catch(error => {
		error('Fehler beim Hinzufügen der Property:', error);
	});
}

const savedQueriesMap = new Map();

function loadSavedQueriesFromAPI() {
	fetch('/api/get_saved_queries')
		.then(response => response.json())
		.then(data => {
			const selectElement = document.getElementById('savedQueriesSelect');
			selectElement.innerHTML = '<option value="">-- Wähle eine gespeicherte Abfrage --</option>';

			data.forEach(query => {
				savedQueriesMap.set(query.name, query); // Map speichert die Query
				const option = document.createElement('option');
				option.value = query.name; // nur Name im HTML
				option.textContent = query.name;
				selectElement.appendChild(option);
			});
		})
		.catch(error => console.error('Fehler beim Laden gespeicherter Abfragen:', error));
}

function getQBFromURL() {
	try {
		const params = new URLSearchParams(window.location.search);
		const qbParam = params.get('qb');
		if (!qbParam) return null;
		return JSON.parse(decodeURIComponent(qbParam));
	} catch (e) {
		error('Ungültiges QB-JSON in URL: ' + e.message);
		return null;
	}
}

function saveQuery() {
    const name = document.getElementById('queryNameInput').value;

    if (!name) {
        error('Bitte gib einen Namen ein.');
        return;
    }

    // komplette URL als String
    const url = window.location.href;

    fetch('/api/save_query', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: name, url: url })
    })
    .then(res => res.json())
    .then(data => {
        if (data.status === 'success') {
            success(data.message);
            document.getElementById('queryNameInput').value = '';
            loadSavedQueriesFromAPI();
        } else {
            error(data.message);
        }
    })
    .catch(err => error('Fehler beim Speichern der Query: ' + err.message));
}

function loadSavedQuery() {
	const name = $("#savedQueriesSelect").val();

	if (!name) {
		error('Bitte wähle eine gespeicherte Query aus.');
		return;
	}

	fetch(`/api/get_query_by_name?name=${encodeURIComponent(name)}`)
		.then(res => res.json())
		.then(data => {
			if (data.status === 'success') {
				const savedURL = data.query.url;
				if (!savedURL) {
					error('Gespeicherte Query enthält keine URL.');
					return;
				}
				// Seite einfach mit der gespeicherten URL neu laden
				window.location.href = savedURL;
			} else {
				error(data.message);
			}
		})
		.catch(err => error('Fehler beim Laden der Query: ' + err.message));
}

function fetchRelationships() {
	fetch('/api/relationships')
		.then(response => response.json())
		.then(data => {
			const container = document.getElementById('relationshipSelection');
			container.innerHTML = ''; // vorherige Inhalte löschen
			data.forEach(rel => {
				const label = document.createElement('label');
				label.style.display = 'block';

				const checkbox = document.createElement('input');
				checkbox.type = 'checkbox';
				checkbox.name = 'relationship';
				checkbox.value = rel;
				checkbox.checked = true; // standardmäßig ausgewählt

				label.appendChild(checkbox);
				label.appendChild(document.createTextNode(' ' + rel));

				container.appendChild(label);
			});
		})
		.catch(err => console.error('Fehler beim Laden der Relationships:', err));
}

function getSelectedLabels(container) {
	return Array.from(container.querySelectorAll('input[type="checkbox"]:checked'))
		.map(cb => cb.value);
}

// === Utility: gewählte Relationships ===
function getSelectedRelationships() {
	return Array.from(document.querySelectorAll('#relationshipSelection input[type="checkbox"]:checked'))
		.map(cb => cb.value);
}

function getQueryBuilderRules() {
	try {
		if ($('#querybuilder').length && $('#querybuilder').queryBuilder) {
			const rules = $('#querybuilder').queryBuilder('getRules');
			// rules kann null oder leer sein
			if (!rules || !rules.rules || rules.rules.length === 0) {
				return null;
			}
			return rules;
		}
	} catch (e) {
		// Ignoriere Fehler, z. B. wenn QueryBuilder noch nicht initiiert ist
		console.warn('QueryBuilder noch nicht initialisiert oder keine Regeln vorhanden.');
	}
	return null;
}

function createButtonWithHandler(container, text, onClick) {
	const btn = document.createElement('button');
	btn.textContent = text;
	btn.style.marginBottom = '10px';
	btn.onclick = onClick;
	container.parentNode.insertBefore(btn, container);
	return btn;
}

function createButton(text, onClick) {
	const btn = document.createElement('button');
	btn.textContent = text;
	btn.style.marginBottom = '10px';
	btn.onclick = onClick;
	return btn;
}

function insertBefore(container, element) {
	container.parentNode.insertBefore(element, container);
}


// Initial beim Laden der Seite
document.addEventListener('DOMContentLoaded', () => {
	initQueryBuilder();

	loadSavedQueriesFromAPI();

	restoreStateFromUrl();

	createButtonWithHandler(resultsContainer, 'Neue Zeile hinzufügen', addRowToTable);

	const insertBtn = createButton('Neue Zeile hinzufügen', addRowToTable);
	insertBefore(resultsContainer, insertBtn);

	fetchRelationships();
});

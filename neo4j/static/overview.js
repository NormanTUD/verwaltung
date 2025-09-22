const querySelection = document.getElementById('querySelection');
const resultsContainer = document.getElementById('resultsContainer');

function fetchData() {
  var sel = document.getElementById('querySelection');
  if (!sel) return alert('Kein #querySelection im DOM');

  var labels = [].slice.call(sel.querySelectorAll('input:checked')).map(function(i){ return i.value; });
  if (!labels.length) { alert('Bitte mindestens ein Label auswählen'); return; }

  var qs = 'nodes=' + encodeURIComponent(labels.join(','));
  var url = '/api/get_data_as_table?' + qs;

  fetch(url, { method: 'GET', headers: { 'Accept': 'application/json' } })
    .then(function(res){
      if (!res.ok) throw new Error('Server antwortete mit ' + res.status);
      return res.json();
    })
    .then(function(data){
      if (data && data.status === 'error') {
        alert(data.message || 'Fehler vom Server');
        return;
      }
      renderTable(data);
    })
    .catch(function(err){
      console.error('fetchData error', err);
      alert('Fehler beim Laden: ' + (err.message || err));
    });
}

function renderTable(data) {
  var container = document.getElementById('resultsContainer');
  if (!container) return console.error('renderTable: kein #resultsContainer');

  container.innerHTML = '';

  var cols = data.columns || [];
  var rows = data.rows || [];

  var table = document.createElement('table');
  table.className = 'query-results-table';

  table.appendChild(make_thead_from_columns(cols));
  var tbody = document.createElement('tbody');

  rows.forEach(function(row) {
    // build node map from columns + cells (index-aligned)
    var node_map = build_node_map_from_row(cols, row.cells || []);

    // single visual row per row: each column cell -> input
    var tr = document.createElement('tr');

    for (var i = 0; i < cols.length; ++i) {
      var col = cols[i];
      var cell = (row.cells && row.cells[i]) ? row.cells[i] : null;
      tr.appendChild(make_input_td(cell, col));
    }

    // relations column
    var td_rel = document.createElement('td');
    td_rel.innerHTML = format_relations_html(row.relations || [], node_map);
    tr.appendChild(td_rel);

    // plus button
    var td_plus = document.createElement('td');
    var btn_plus = document.createElement('button');
    btn_plus.type = 'button';
    btn_plus.setAttribute('onclick', 'addColumnToNode(event)');
    btn_plus.textContent = '+';
    td_plus.appendChild(btn_plus);
    tr.appendChild(td_plus);

    // action (delete) button
    var td_act = document.createElement('td');
    var btn_del = document.createElement('button');
    btn_del.type = 'button';
    btn_del.className = 'delete-btn';
    btn_del.setAttribute('data-id', first_node_id_from_row(row));
    btn_del.textContent = 'Löschen';
    btn_del.addEventListener('click', function (ev) {
      var id = ev.currentTarget.getAttribute('data-id');
      handle_delete_node_by_id(id, ev.currentTarget);
    });
    td_act.appendChild(btn_del);
    tr.appendChild(td_act);

    tbody.appendChild(tr);
  });

  table.appendChild(tbody);
  container.appendChild(table);
}

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
    th.textContent = multi_types ? (c.nodeType + ':' + c.property) : c.property;
    tr.appendChild(th);
  }

  var thR = document.createElement('th'); thR.textContent = 'Beziehungen'; tr.appendChild(thR);
  var thPlus = document.createElement('th'); thPlus.textContent = '+'; tr.appendChild(thPlus);
  var thAct = document.createElement('th'); thAct.textContent = 'Aktion'; tr.appendChild(thAct);
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

function handle_delete_node_by_id(id, btnEl) {
  if (!id) return;
  if (typeof window.deleteNode === 'function') {
    try { window.deleteNode(id); } catch (e) { console.error(e); }
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
	const nodeIds = event.target.getAttribute('data-id').split(',');
	//if (confirm(`Sicher, dass du die verknüpften Einträge (${nodeIds.join(', ')}) löschen möchtest?`)) {
	fetch(`/api/delete_nodes?ids=${nodeIds.join(',')}`, {
		method: 'DELETE',
	}).then(response => {
		if (!response.ok) {
			throw new Error(`HTTP error! status: ${response.status}`);
		}
		return response.json();
	})
		.then(data => {
			console.log(data.message);
			if (data.status === 'success') {
				fetchData();
			}
		})
		.catch(error => console.error('Fehler beim Löschen:', error));
	//}
}

function updateValue(element) {
	try {
		if (!element) {
			console.error('updateValue wurde ohne Element aufgerufen.');
			return;
		}

		// Attribute auslesen
		const dataIdAttr = element.getAttribute('data-id');
		const propertyName = element.getAttribute('data-property');

		if (!dataIdAttr) {
			console.error('Fehlendes "data-id"-Attribut auf dem Element:', element);
			return;
		}
		if (!propertyName) {
			console.error('Fehlendes "data-property"-Attribut auf dem Element:', element);
			return;
		}

		// IDs parsen
		const combinedIds = dataIdAttr.split(',').map(idStr => {
			const idNum = Number(idStr.trim());
			if (isNaN(idNum)) {
				throw new Error(`Ungültige ID "${idStr}" in "data-id" gefunden. Erwartet: Zahl.`);
			}
			return idNum;
		});

		// Wert prüfen
		const newValue = element.value;
		if (element.originalValue === newValue) {
			console.log('Kein Update nötig, Wert unverändert:', newValue);
			return;
		}
		element.originalValue = newValue;

		// Logging vor dem Request
		console.log('Sende Update für IDs:', combinedIds);
		console.log('Property:', propertyName);
		console.log('Neuer Wert:', newValue);

		// Fetch request
		fetch('/api/update_nodes', {
			method: 'PUT',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify({
				ids: combinedIds,
				property: propertyName,
				value: newValue
			})
		})
		.then(async response => {
			let responseBody;
			try {
				responseBody = await response.json();
			} catch (jsonError) {
				throw new Error(`Antwort konnte nicht als JSON geparsed werden. HTTP-Status: ${response.status}`);
			}

			if (!response.ok) {
				const serverMessage = responseBody?.message || 'Keine Nachricht vom Server.';
				throw new Error(`Update fehlgeschlagen. HTTP-Status: ${response.status}. Server meldet: ${serverMessage}. Gesendet: ${JSON.stringify({ ids: combinedIds, property: propertyName, value: newValue })}`);
			}

			return responseBody;
		})
		.then(data => {
			if (!data || !data.message) {
				console.warn('Antwort ohne "message"-Feld erhalten:', data);
			} else {
				console.log('Update erfolgreich:', data.message);
			}
		})
		.catch(error => {
			console.error('Fehler beim Aktualisieren:', error.message);
		});

	} catch (err) {
		console.error('Fehler in updateValue-Funktion:', err.message, err);
	}
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
		alert("Keine IDs in dieser Zeile gefunden!");
		return;
	}

	// Aktive Labels aus #querySelection
	const activeLabels = Array.from(document.querySelectorAll('#querySelection input[type="checkbox"]:checked'))
		.map(cb => cb.value);

	if (activeLabels.length === 0) {
		alert("Bitte mindestens einen Node-Typ im Bereich 'querySelection' auswählen!");
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
			alert("Bitte einen Spaltennamen eingeben!");
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
				console.log(data.message);
				if (data.status === 'success') {
					fetchData();
				}
			})
			.catch(error => {
				console.error('Fehler beim Hinzufügen der Spalte:', error);
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
		console.error('Fehler beim Hinzufügen der Property:', error);
	});
}

// Neue Funktionen für das Speichern und Laden von Abfragen
function loadSavedQueriesFromAPI() {
	fetch('/api/get_saved_queries')
		.then(response => response.json())
		.then(data => {
			const selectElement = document.getElementById('savedQueriesSelect');
			selectElement.innerHTML = '<option value="">-- Wähle eine gespeicherte Abfrage --</option>';
			if (data && data.length > 0) {
				data.forEach(query => {
					const option = document.createElement('option');
					option.value = JSON.stringify(query.labels);
					option.textContent = query.name;
					selectElement.appendChild(option);
				});
			}
		})
		.catch(error => console.error('Fehler beim Laden gespeicherter Abfragen:', error));
}

function saveQuery() {
	const name = document.getElementById('queryNameInput').value;
	const selectedLabels = [...querySelection.querySelectorAll('input:checked')]
		.map(input => input.value);

	if (!name || selectedLabels.length === 0) {
		alert('Bitte gib einen Namen ein und wähle mindestens ein Label aus.');
		return;
	}

	fetch('/api/save_query', {
		method: 'POST',
		headers: { 'Content-Type': 'application/json' },
		body: JSON.stringify({ name, selectedLabels })
	})
		.then(response => response.json())
		.then(data => {
			alert(data.message);
			if (data.status === 'success') {
				document.getElementById('queryNameInput').value = '';
				loadSavedQueriesFromAPI();
			}
		})
		.catch(error => console.error('Fehler beim Speichern der Abfrage:', error));
}

function loadSavedQuery() {
	const selectElement = document.getElementById('savedQueriesSelect');
	const selectedLabelsJson = selectElement.value;

	if (!selectedLabelsJson) {
		return;
	}

	querySelection.querySelectorAll('input[type="checkbox"]').forEach(input => {
		input.checked = false;
	});

	const labelsToSelect = JSON.parse(selectedLabelsJson);
	labelsToSelect.forEach(label => {
		const checkbox = querySelection.querySelector(`input[value="${label}"]`);
		if (checkbox) {
			checkbox.checked = true;
		}
	});

	fetchData();
}

// Initial beim Laden der Seite
document.addEventListener('DOMContentLoaded', () => {
	loadSavedQueriesFromAPI();

	document.getElementById('querySelection').addEventListener('change', fetchData);
});

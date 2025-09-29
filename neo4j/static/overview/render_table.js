"use strict";

// === Fetch-Daten + URL-State ===
function fetchData(updateUrl = true) {
	var sel = document.getElementById('querySelection');
	if (!sel) {
		error('Kein #querySelection im DOM');
		return;
	}

	var labels = getSelectedLabels(sel);
	if (!labels.length) {
		warning('Bitte mindestens ein Label auswählen'); 
		return;
	}

	var relationships = getSelectedRelationships();
	var qbRules = getQueryBuilderRules();

	// QueryBuilder-Regeln als JSON-String
	var qbJson = qbRules ? JSON.stringify(qbRules) : '';

	// URL-Parameter bauen
	var params = new URLSearchParams();

	if (labels.length) {
		params.set('nodes', labels.join(','));
	}

	if (relationships.length) {
		params.set('relationships', relationships.join(','));
	}

	if (qbJson) {
		params.set('qb', qbJson);
	}

	// URL aktualisieren
	if (updateUrl) {
		var newUrl = window.location.pathname + '?' + params.toString();
		history.replaceState(null, '', newUrl); // ersetzt aktuelle URL ohne Reload
	}

	// API-Call
	var url = '/api/get_data_as_table?' + params.toString();

	fetch(url, {
		method: 'GET',
		headers: { 'Accept': 'application/json' }
	})
		.then(handleFetchResponse)
		.then(handleServerData)
		.catch(function (err) {
			error('Fehler beim Laden: ' + (err.message || err));
		});
}

// ----------------- Table Rendering -----------------

function renderTable(data) {
	var container = document.getElementById('resultsContainer');
	if (!container) {
		error('renderTable: kein #resultsContainer');
		return;
	}

	container.innerHTML = '';

	var cols = data.columns || [];
	var rows = data.rows || [];

	var table = document.createElement('table');
	table.className = 'query-results-table';

	table.appendChild(make_thead_from_columns(cols));
	table.appendChild(makeTableBody(cols, rows));

	container.appendChild(table);
}

function makeTableBody(cols, rows) {
	var tbody = document.createElement('tbody');

	rows.forEach(function (row) {
		tbody.appendChild(makeRow(cols, row));
	});

	return tbody;
}

function makeRow(cols, row) {
	var tr = document.createElement('tr');
	var node_map = build_node_map_from_row(cols, row.cells || []);
	var rowRelationsData = encodeURIComponent(JSON.stringify(row.relations || []));

	cols.forEach(function (col, i) {
		tr.appendChild(makeCell(col, row, i, rowRelationsData));
	});

	tr.appendChild(makeRelationsCell(row, node_map));
	tr.appendChild(makePlusCell());
	tr.appendChild(makeDeleteCell(row));

	return tr;
}

function makeCell(col, row, i, rowRelationsData) {
	var cell = (row.cells && row.cells[i]) ? row.cells[i] : null;
	var td = make_input_td(cell, col);
	td.setAttribute('data-relations', rowRelationsData);
	return td;
}

function makeRelationsCell(row, node_map) {
	var td = document.createElement('td');
	td.innerHTML = format_relations_html(row.relations || [], node_map);
	return td;
}

function makePlusCell() {
	var td = document.createElement('td');
	var btn = document.createElement('button');
	btn.type = 'button';
	btn.textContent = '+';
	btn.addEventListener('click', addColumnToNode);
	td.appendChild(btn);
	return td;
}

function makeDeleteCell(row) {
	var td = document.createElement('td');
	td.appendChild(makeDeleteButton(first_node_id_from_row(row)));
	return td;
}

function makeDeleteButton(id) {
	var btn = document.createElement('button');
	btn.type = 'button';
	btn.className = 'delete-btn';
	btn.textContent = 'Löschen';
	btn.setAttribute('data-id', id);
	btn.addEventListener('click', function (ev) {
		var targetId = ev.currentTarget.getAttribute('data-id');
		handle_delete_node_by_id(targetId, ev.currentTarget, ev);
	});
	return btn;
}

// ----------------- Fetch Helpers -----------------

function getSelectedLabels(sel) {
	return [].slice.call(sel.querySelectorAll('input:checked'))
		.map(function (i) {
			return i.value;
		});
}

function buildQueryUrl(labels) {
	var qs = 'nodes=' + encodeURIComponent(labels.join(','));
	return '/api/get_data_as_table?' + qs;
}

function handleFetchResponse(res) {
	if (!res.ok) {
		throw new Error('Server antwortete mit ' + res.status);
	}
	return res.json();
}

function handleServerData(data) {
	if (data && data.status === 'error') {
		error(data.message || 'Fehler vom Server');
		return;
	}

	collectGlobalRelations(data);
	renderTable(data);
}

function collectGlobalRelations(data) {
	if (!data || !Array.isArray(data.rows)) {
		warning('Keine gültigen Rows für Relations');
		return;
	}

	window.globalRelations = [];

	// Mapping nodeId -> nodeType
	var nodeIdToType = {};

	data.rows.forEach(function (row) {
		if (Array.isArray(row.cells)) {
			row.cells.forEach(function (cell, idx) {
				if (cell && cell.nodeId !== null && cell.nodeId !== undefined) {
					var col = data.columns[idx];
					if (col && col.nodeType) {
						nodeIdToType[cell.nodeId] = col.nodeType;
					}
				}
			});
		}
	});

	// Relations mit Typ-Info aufbauen
	data.rows.forEach(function (row) {
		if (Array.isArray(row.relations)) {
			row.relations.forEach(function (rel) {
				if (rel && rel.fromId !== undefined && rel.toId !== undefined) {
					window.globalRelations.push({
						fromId: rel.fromId,
						fromType: nodeIdToType[rel.fromId] || null,
						toId: rel.toId,
						toType: nodeIdToType[rel.toId] || null,
						relation: rel.relation
					});
				}
			});
		}
	});
}

document.getElementById('querySelection').addEventListener('change', fetchData);
document.getElementById('relationshipSelection').addEventListener('change', fetchData);

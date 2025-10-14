"use strict";

// ---------- Utilities ----------
function selectTbody(selector = ".query-results-table tbody") {
	return document.querySelector(selector);
}

function selectHeaders(selector = ".query-results-table thead th") {
	return Array.from(document.querySelectorAll(selector))
		.map(th => th.textContent.trim())
		.filter(h => !['+', 'Aktion', 'Beziehungen'].includes(h));
}

function parseHeader(header) {
	const [label, property] = header.split(":");
	return { label, property };
}

function mapHeaders(headers) {
	const map = {};
	headers.forEach((h, i) => {
		map[h] = { index: i, ...parseHeader(h) };
	});
	return map;
}

// ---------- Node Handling ----------
function getLabelsForRow(globalRelations) {
	// Labels dieser Zeile (nur aus globalRelations) sammeln
	const labels = new Set();
	globalRelations.forEach(r => {
		if (r.fromType) labels.add(r.fromType);
		if (r.toType) labels.add(r.toType);
	});
	return Array.from(labels);
}

function initializeNodesForRow(headersMap, labels) {
	const nodesPerLabel = {};
	labels.forEach(label => {
		nodesPerLabel[label] = {};
		for (let header in headersMap) {
			if (headersMap[header].label === label) {
				nodesPerLabel[label][headersMap[header].property] = "";
			}
		}
	});
	return nodesPerLabel;
}

async function createNode(label, properties) {
	const res = await fetch("/api/add_row", {
		method: "POST",
		headers: { "Content-Type": "application/json" },
		body: JSON.stringify({ label, properties })
	});
	if (!res.ok) throw new Error(`Failed to create node for label: ${label}`);
	const data = await res.json();
	return data.id;
}

async function createAllNodes(nodesPerLabel) {
	const nodeIds = {};
	for (let label in nodesPerLabel) {
		nodeIds[label] = await createNode(label, nodesPerLabel[label]);
	}
	return nodeIds;
}

async function createRelationshipsForRow(globalRelations, nodeIds) {
	const createdRels = [];
	for (let r of globalRelations) {
		const startId = nodeIds[r.fromType];
		const endId = nodeIds[r.toType];
		if (!startId || !endId) continue; // Skip if Node nicht erzeugt

		const res = await fetch("/api/add_relationship", {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify({
				start_id: startId,
				end_id: endId,
				type: r.relation,
				props: r.props || {}
			})
		});
		if (!res.ok) console.error("Failed to create relationship:", r);
		else {
			const data = await res.json();
			createdRels.push(data);
		}
	}
	return createdRels;
}

// ---------- DOM Creation ----------
function createInputCell(label, property, id) {
	const td = document.createElement("td");
	const input = document.createElement("input");
	input.type = "text";
	input.dataset.label = label;
	input.dataset.property = property;
	input.dataset.id = id;
	input.onblur = function () { updateValue(this); };
	td.appendChild(input);
	return td;
}


function createPlusButtonCell() {
	const td = document.createElement("td");
	const btn = document.createElement("button");
	btn.type = "button";
	btn.textContent = "+";
	btn.onclick = addColumnToNode;
	td.appendChild(btn);
	return td;
}

function createActionCell() {
	return document.createElement("td");
}

function buildRow(headers, headersMap, nodeIds, relationships) {
	const tr = document.createElement("tr");
	headers.forEach(header => {
		const { label, property } = headersMap[header];
		const id = nodeIds[label] || "";
		tr.appendChild(createInputCell(label, property, id));
	});
	tr.appendChild(createPlusButtonCell());
	tr.appendChild(createActionCell());
	return tr;
}

// ---------- Main ----------
async function addRowToTable() {
	const tbody = selectTbody();
	if (!tbody) return;

	try {
		const headers = selectHeaders();
		const headersMap = mapHeaders(headers);

		// 1. Labels für diese Zeile
		const labels = getLabelsForRow(globalRelations);

		// 2. Nodes initialisieren
		const nodesPerLabel = initializeNodesForRow(headersMap, labels);

		// 3. Nodes erzeugen
		const nodeIds = await createAllNodes(nodesPerLabel);

		// 4. Beziehungen erzeugen
		const relationships = await createRelationshipsForRow(globalRelations, nodeIds);

		// 5. Zeile bauen und einfügen
		const tr = buildRow(headers, headersMap, nodeIds, relationships);
		tbody.appendChild(tr);

		open_link("/overview" + window.location.search);
	} catch (err) {
		console.error("Error adding row:", err);
	}
}

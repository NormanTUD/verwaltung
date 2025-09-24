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

// ---------- Node Handling ----------
function initializeNodes(headers) {
    const nodesPerLabel = {};
    for (let header of headers) {
        const { label, property } = parseHeader(header);
        if (!nodesPerLabel[label]) nodesPerLabel[label] = {};
        nodesPerLabel[label][property] = "";
    }
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

function createRelationshipCell() {
    return document.createElement("td");
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

function buildRow(headers, nodeIds) {
    const tr = document.createElement("tr");
    for (let header of headers) {
        const { label, property } = parseHeader(header);
        tr.appendChild(createInputCell(label, property, nodeIds[label]));
    }
    tr.appendChild(createRelationshipCell());
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
        const nodesPerLabel = initializeNodes(headers);
        const nodeIds = await createAllNodes(nodesPerLabel);
        const tr = buildRow(headers, nodeIds);
        tbody.appendChild(tr);
    } catch (err) {
        console.error("Error adding row:", err);
    }
}

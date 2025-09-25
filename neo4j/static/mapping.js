        const nodeListDiv = document.getElementById('nodeList');
        const relationshipListDiv = document.getElementById('relationshipList');
        const assignedColumns = new Set();
        let nodeCounter = 0;
        let existingRelTypes = [];

	const nodeSuggestions = {
		'Buch': ['buchtitel', 'erscheinungsjahr'],
		'Person': ['titel', 'telefon', 'vorname', 'nachname', 'name', 'person', 'first_name', 'last_name', 'kontaktdaten'],
		'ZIH-Login': ['ZIH-Login', 'Ablauf'],
		'Stadt': ['stadt', 'land', 'city', 'country'],
		'Ort': ['ort', 'adresse', 'plz', 'strasse', 'straße', 'zip'],
		'Struktureinheit': ['struktureinheit'],
		'Firma': ['firma', 'unternehmen', 'company'],
		'Raum': ['raum'],
		'Abteilung': ['abteilung']
	};

        async function fetchExistingRelTypes() {
            try {
                const response = await fetch('/get_rel_types');
                if (response.ok) {
                    existingRelTypes = await response.json();
                }
            } catch (e) {
                console.error("Konnte existierende Relationship-Typen nicht laden:", e);
            }
        }

        function updateColumnStates() {
            assignedColumns.clear();
            document.querySelectorAll('.node-group').forEach(group => {
                const selectedCols = group.querySelectorAll('.column-item.selected');
                selectedCols.forEach(col => assignedColumns.add(col.dataset.column));
            });

            document.querySelectorAll('.column-item').forEach(col => {
                const column = col.dataset.column;
                const isSelectedInAnyGroup = assignedColumns.has(column);
                const isSelectedInThisGroup = col.classList.contains('selected');

                if (isSelectedInAnyGroup && !isSelectedInThisGroup) {
                    col.classList.add('disabled');
                    col.classList.remove('selected');
                } else {
                    col.classList.remove('disabled');
                }
            });
            updateRelationshipSelects();
        }

        function addNodeMapping(suggestedType = '') {
            nodeCounter++;
            const newNodeDiv = document.createElement('div');
            newNodeDiv.classList.add('node-group');
            
            let nodeLabel = suggestedType || `Node-Typ ${nodeCounter}`;
            let selectedHeaders = [];

            if (suggestedType) {
                const keywords = nodeSuggestions[suggestedType] || [];
                selectedHeaders = headers.filter(h => 
                    !assignedColumns.has(h) && keywords.some(keyword => h.toLowerCase().includes(keyword))
                );
            }
            
            const columnItemsHtml = headers.map(h => {
                const hClean = h.toLowerCase().replace(/[^a-zA-Zäöüß0-9]/g, '');
                const isPreselected = selectedHeaders.includes(h);
                const isAlreadyAssigned = assignedColumns.has(h);
                const disabledClass = isAlreadyAssigned && !isPreselected ? 'disabled' : '';
                const selectedClass = isPreselected ? 'selected' : '';
                const propName = hClean;
                return `<div class="column-item ${selectedClass} ${disabledClass}" data-column="${h}">
                            <span>${h}</span>
                            <input type="text" class="property-input" placeholder="${propName}" value="${propName}" onclick="event.stopPropagation()">
                        </div>`;
            }).join('');

            newNodeDiv.innerHTML = `
                <h4 contenteditable="true" class="node-label-input" placeholder="Node-Label" onblur="updateRelationshipSelects()">${nodeLabel}</h4>
                <div class="column-list">
                    ${columnItemsHtml}
                </div>
                <span class="remove-button" onclick="this.closest('.node-group').remove(); updateColumnStates();">Entfernen</span>
            `;
            nodeListDiv.appendChild(newNodeDiv);
            
            newNodeDiv.querySelectorAll('.column-item').forEach(col => {
                col.addEventListener('click', (event) => {
                    if (col.classList.contains('disabled')) {
                        alert('Diese Spalte ist bereits einem anderen Node-Typen zugeordnet.');
                        return;
                    }
                    col.classList.toggle('selected');
                    updateColumnStates();
                });
            });
            updateColumnStates();
        }

        function updateRelationshipSelects() {
            const availableNodes = [...document.querySelectorAll('.node-label-input')]
                                            .map(input => input.textContent.trim())
                                            .filter(label => label.length > 0);
            document.querySelectorAll('.node-select').forEach(select => {
                const currentValue = select.value;
                select.innerHTML = `<option value="">-- Wähle Node --</option>` +
                                   availableNodes.map(label => `<option value="${label}">${label}</option>`).join('');
                select.value = currentValue;
            });
        }

        function addRelationship() {
            const newRelDiv = document.createElement('div');
            newRelDiv.classList.add('relationship-group');

            let optionsHtml = existingRelTypes.map(rel => `<option value="${rel}">${rel}</option>`).join('');
            optionsHtml += `<option value="__new__">+ Neuer Typ...</option>`;

            newRelDiv.innerHTML = `
                <h4>Neue Relationship</h4>
                <label>Von:</label>
                <select class="from-node-select node-select"></select>
                <label>Nach:</label>
                <select class="to-node-select node-select"></select>
                <br><br>
                <label>Relationship-Typ:</label>
                <select class="rel-type-select">${optionsHtml}</select>
                <input type="text" class="rel-type-input" placeholder="z.B. HAT_ZUGEHÖRIGKEIT">
                <span class="remove-button" onclick="this.closest('.relationship-group').remove()">Entfernen</span>
            `;
            relationshipListDiv.appendChild(newRelDiv);
            updateRelationshipSelects();

            const selectEl = newRelDiv.querySelector('.rel-type-select');
            const inputEl = newRelDiv.querySelector('.rel-type-input');

            // Event wenn man zwischen vorhandenen Typen und "+ Neuer Typ..." wechselt
            selectEl.addEventListener('change', () => {
                if (selectEl.value === '__new__') {
                    inputEl.style.display = 'inline-block';
                    inputEl.value = '';
                    inputEl.focus();
                } else {
                    inputEl.style.display = 'none';
                    inputEl.value = selectEl.value;
                }
            });

            // FALLS initial nur "+ Neuer Typ..." vorhanden ist, Input direkt anzeigen
            if (existingRelTypes.length === 0) {
                selectEl.value = '__new__';
                inputEl.style.display = 'inline-block';
                inputEl.focus();
            }
        }

        function autoSuggestNodes() {
            const suggestedTypes = new Set();
            headers.forEach(h => {
                const hClean = h.toLowerCase().replace(/[^a-z0-9]/g, '');
                for (const type in nodeSuggestions) {
                    const keywords = nodeSuggestions[type];
                    if (keywords.some(keyword => hClean.includes(keyword))) {
                        suggestedTypes.add(type);
                    }
                }
            });

            document.getElementById('nodeList').innerHTML = '';
            suggestedTypes.forEach(type => addNodeMapping(type));
            if (suggestedTypes.size === 0) {
                addNodeMapping();
            }
        }
        
        document.addEventListener('DOMContentLoaded', async () => {
             await fetchExistingRelTypes();
             autoSuggestNodes();
             addRelationship();
        });

        function saveMapping() {
            const mapping = { nodes: {}, relationships: [] };
            let hasError = false;
            let relationshipErrors = [];

            document.querySelectorAll('.node-group').forEach(group => {
                const label = group.querySelector('.node-label-input').textContent.trim();
                const fields = [...group.querySelectorAll('.column-item.selected')].map(item => {
                    const original = item.dataset.column;
                    const renamed = item.querySelector('.property-input').value.trim();
                    return { original, renamed: renamed || original };
                });
                if (label && fields.length > 0) {
                    mapping.nodes[label] = fields;
                }
            });

            const assignedHeadersCount = Object.values(mapping.nodes).flat().length;
            if (assignedHeadersCount !== headers.length) {
                alert('Fehler: Alle Spalten müssen einem Node-Typen zugeordnet werden.');
                hasError = true;
            }

            document.querySelectorAll('.relationship-group').forEach((group, idx) => {
                const from = group.querySelector('.from-node-select').value;
                const to = group.querySelector('.to-node-select').value;
                const typeInput = group.querySelector('.rel-type-input').value.trim();

                // Vorher evtl. rote Markierungen zurücksetzen
                group.style.borderColor = '#eee';

                if (from && to && typeInput) {
                    mapping.relationships.push({ from, to, type: typeInput });
                } else if (from || to || typeInput) {
                    hasError = true;
                    let missing = [];
                    if (!from) missing.push("Von-Node");
                    if (!to) missing.push("Nach-Node");
                    if (!typeInput) missing.push("Relationship-Typ");

                    relationshipErrors.push(`Relationship ${idx + 1}: fehlt ${missing.join(', ')}`);

                    // visuelles Feedback
                    group.style.borderColor = 'red';
                }
            });

            const numNodeTypes = Object.keys(mapping.nodes).length;
            if (numNodeTypes > 1 && mapping.relationships.length === 0) {
                alert('Fehler: Bei mehreren Node-Typen muss mindestens eine Relationship definiert werden.');
                hasError = true;
            }

            if (relationshipErrors.length > 0) {
                alert("Fehlerhafte Relationships:\n" + relationshipErrors.join("\n"));
            }

            if (hasError) {
                return;
            }

            fetch('/save_mapping', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(mapping)
            }).then(response => response.json())
            .then(data => {
                alert(data.message);
                if (data.status === 'success') {
                    window.location.href = '/overview';
                }
            });
        }

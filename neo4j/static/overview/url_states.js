// === Fetch-Daten + URL-State ===
function fetchData(updateUrl = true) {
    var sel = document.getElementById('querySelection');
    if (!sel) { error('Kein #querySelection im DOM'); return; }

    var labels = getSelectedLabels(sel);
    if (!labels.length) { warning('Bitte mindestens ein Label auswählen'); return; }

    var relationships = getSelectedRelationships();
    var qbRules = getQueryBuilderRules();

    // QueryBuilder-Regeln als JSON-String
    var qbJson = qbRules ? JSON.stringify(qbRules) : '';

    // URL-Parameter bauen
    var params = new URLSearchParams();
    if (labels.length) params.set('nodes', labels.join(','));
    if (relationships.length) params.set('relationships', relationships.join(','));
    if (qbJson) params.set('qb', qbJson);

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

// === State beim Laden wiederherstellen ===
function restoreStateFromUrl() {
    var params = new URLSearchParams(window.location.search);

    // Labels
    var nodes = params.get('nodes');
    if (nodes) {
        var labelArray = nodes.split(',');
        var sel = document.getElementById('querySelection');
        if (sel) {
            sel.querySelectorAll('input[type="checkbox"]').forEach(cb => {
                cb.checked = labelArray.includes(cb.value);
            });
        }
    }

    // Relationships
    var rels = params.get('relationships');
    if (rels) {
        var relArray = rels.split(',');
        var container = document.getElementById('relationshipSelection');
        if (container) {
            container.querySelectorAll('input[type="checkbox"]').forEach(cb => {
                cb.checked = relArray.includes(cb.value);
            });
        }
    }

    // QueryBuilder-Regeln
    var qb = params.get('qb');
    if (qb) {
        try {
            var rules = JSON.parse(qb);
            if ($('#querybuilder').length && $('#querybuilder').queryBuilder) {
                $('#querybuilder').queryBuilder('setRules', rules);
            }
        } catch (e) {
            console.warn('Fehler beim Wiederherstellen der QueryBuilder-Regeln', e);
        }
    }
}

"use strict";

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

// === direkt beim Laden aufrufen ===
document.addEventListener('DOMContentLoaded', function() {
    restoreStateFromUrl();

    restoreQueryBuilderFromUrl();

    fetchData(false); // lädt Daten ohne URL erneut zu ändern
});


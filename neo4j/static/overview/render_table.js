function fetchData() {
    var sel = document.getElementById('querySelection');
    if (!sel) {
        error('Kein #querySelection im DOM');
        return;
    }

    var labels = [].slice.call(sel.querySelectorAll('input:checked'))
        .map(function (i) {
            return i.value;
        });

    if (!labels.length) {
        warning('Bitte mindestens ein Label auswählen');
        return;
    }

    var qs = 'nodes=' + encodeURIComponent(labels.join(','));
    var url = '/api/get_data_as_table?' + qs;

    fetch(url, {
        method: 'GET',
        headers: { 'Accept': 'application/json' }
    })
        .then(function (res) {
            if (!res.ok) {
                throw new Error('Server antwortete mit ' + res.status);
            }
            return res.json();
        })
        .then(function (data) {
            if (data && data.status === 'error') {
                error(data.message || 'Fehler vom Server');
                return;
            }
            renderTable(data);
        })
        .catch(function (err) {
            error('Fehler beim Laden: ' + (err.message || err));
        });
}

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

    var tbody = document.createElement('tbody');

    rows.forEach(function (row) {
        var node_map = build_node_map_from_row(cols, row.cells || []);
        var tr = document.createElement('tr');

        // Jede Zelle in der Zeile erhält ein data-Attribut mit ALLEN Relationen der Zeile
        var rowRelationsData = encodeURIComponent(JSON.stringify(row.relations || []));

        for (var i = 0; i < cols.length; ++i) {
            var col = cols[i];
            var cell = (row.cells && row.cells[i]) ? row.cells[i] : null;
            var td = make_input_td(cell, col);
            td.setAttribute('data-relations', rowRelationsData); // Das ist der Schlüssel!
            tr.appendChild(td);
        }

        var td_rel = document.createElement('td');
        td_rel.innerHTML = format_relations_html(row.relations || [], node_map);
        tr.appendChild(td_rel);

        var td_plus = document.createElement('td');
        var btn_plus = document.createElement('button');
        btn_plus.type = 'button';
        btn_plus.setAttribute('onclick', 'addColumnToNode(event)');
        btn_plus.textContent = '+';
        td_plus.appendChild(btn_plus);
        tr.appendChild(td_plus);

        var td_act = document.createElement('td');
        var btn_del = document.createElement('button');
        btn_del.type = 'button';
        btn_del.className = 'delete-btn';
        btn_del.setAttribute('data-id', first_node_id_from_row(row));
        btn_del.textContent = 'Löschen';
        btn_del.addEventListener('click', function (ev) {
            var id = ev.currentTarget.getAttribute('data-id');
            handle_delete_node_by_id(id, ev.currentTarget, ev);
        });
        td_act.appendChild(btn_del);
        tr.appendChild(td_act);

        tbody.appendChild(tr);
    });

    table.appendChild(tbody);
    container.appendChild(table);
}

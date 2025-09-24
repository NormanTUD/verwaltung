function initQueryBuilder() {
	// Labels laden
	fetch('/api/labels')
		.then(r => r.json())
		.then(labels => {
			// Für jedes Label Properties + Relationen nachladen
			return Promise.all(labels.map(lbl =>
				fetch('/api/properties?label=' + encodeURIComponent(lbl))
				.then(r => r.json())
				.then(props => ({label: lbl, props: props}))
			));
		})
		.then(labelInfos => {
			// Fields für QueryBuilder generieren
			const fields = {};
			labelInfos.forEach(info => {
				info.props.forEach(p => {
					fields[info.label + '.' + p.name] = {
						label: info.label + '.' + p.name,
						type: mapNeoTypeToQB(p.type)
					};
				});
			});

			$('#querybuilder').queryBuilder({
				plugins: ['bt-tooltip-errors'],
				filters: Object.values(fields)
			});
		});
}

function runQueryBuilder() {
	const rules = $('#querybuilder').queryBuilder('getRules');
	if (!rules) {
		alert('Ungültige Query');
		return;
	}

	const url = '/api/get_data_as_table?nodes=' + encodeURIComponent(getSelectedLabels(document.getElementById('querySelection')).join(','));
	fetch(url, {
		method: 'POST',
		headers: {'Content-Type':'application/json','Accept':'application/json'},
		body: JSON.stringify({queryBuilderRules: rules})
	})
		.then(handleFetchResponse)
		.then(handleServerData)
		.catch(err => error('Fehler bei QueryBuilder: ' + (err.message || err)));
}

function mapNeoTypeToQB(neoType) {
	switch(neoType) {
		case 'String': return 'string';
		case 'Integer': return 'integer';
		case 'Float': return 'double';
		case 'Boolean': return 'boolean';
		default: return 'string';
	}
}

document.addEventListener('DOMContentLoaded', initQueryBuilder);

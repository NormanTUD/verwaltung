function initQueryBuilder() {
	fetch('/api/labels')
		.then(r => r.json())
		.then(labels => {
			return Promise.all(labels.map(lbl =>
				fetch('/api/properties?label=' + encodeURIComponent(lbl))
					.then(r => r.json())
					.then(props => ({label: lbl, props: props}))
			));
		})
		.then(labelInfos => {
			const fields = {};
			labelInfos.forEach(info => {
				info.props.forEach(p => {
					const propName = p.name || p.property; // Fallback
					if (!propName) {
						error("Kein Property-Name für", p, "bei Label", info.label);
						return;
					}
					const key = info.label + '.' + propName;
					fields[key] = {
						id: key,
						label: key,
						type: mapNeoTypeToQB(p.type)
					};
				});
			});

			const filters = Object.values(fields);

			try {
				$('#querybuilder').queryBuilder({
					// → entweder Bootstrap einbinden ...
					// plugins: ['bt-tooltip-errors'],
					filters: filters
				});
			} catch (e) {
				error("Fehler beim Init von QueryBuilder:", e);
			}
		})
		.catch(err => {
			error("Fehler in initQueryBuilder:", err);
		});
}


function runQueryBuilder() {
	let rules;

	try {
		rules = $('#querybuilder').queryBuilder('getRules');
	} catch (e) {
		error("Fehler beim getRules:", e);
		return;
	}

	if (!rules) {
		alert('Ungültige Query (keine rules)');
		return;
	}

	const selected = getSelectedLabels(document.getElementById('querySelection'));

	const url = '/api/get_data_as_table?nodes=' + encodeURIComponent(selected.join(','));

	fetch(url, {
		method: 'POST',
		headers: {'Content-Type':'application/json','Accept':'application/json'},
		body: JSON.stringify({queryBuilderRules: rules})
	})
		.then(r => {
			return handleFetchResponse(r);
		})
		.then(data => {
			handleServerData(data);
		})
		.catch(err => {
			error('Fehler bei QueryBuilder: ' + (err.message || err));
		});
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

document.addEventListener('DOMContentLoaded', () => {
	initQueryBuilder();
});

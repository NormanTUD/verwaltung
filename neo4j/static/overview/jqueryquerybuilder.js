function initQueryBuilder() {
	console.log("initQueryBuilder gestartet");

	fetch('/api/labels')
		.then(r => r.json())
		.then(labels => {
			console.log("Labels empfangen:", labels);
			return Promise.all(labels.map(lbl =>
				fetch('/api/properties?label=' + encodeURIComponent(lbl))
					.then(r => r.json())
					.then(props => ({label: lbl, props: props}))
			));
		})
		.then(labelInfos => {
			console.log("Alle LabelInfos gesammelt:", labelInfos);

			const fields = {};
			labelInfos.forEach(info => {
				info.props.forEach(p => {
					const propName = p.name || p.property; // Fallback
					if (!propName) {
						console.error("Kein Property-Name für", p, "bei Label", info.label);
						return;
					}
					const key = info.label + '.' + propName;
					fields[key] = {
						id: key,
						label: key,
						type: mapNeoTypeToQB(p.type)
					};
					console.log("Feld hinzugefügt:", fields[key]);
				});
			});

			const filters = Object.values(fields);
			console.log("Finale Filters für QueryBuilder:", filters);

			try {
				$('#querybuilder').queryBuilder({
					// → entweder Bootstrap einbinden ...
					// plugins: ['bt-tooltip-errors'],
					filters: filters
				});
				console.log("QueryBuilder erfolgreich initialisiert!");
			} catch (e) {
				console.error("Fehler beim Init von QueryBuilder:", e);
			}
		})
		.catch(err => {
			console.error("Fehler in initQueryBuilder:", err);
		});
}


function runQueryBuilder() {
	console.log("runQueryBuilder gestartet");

	let rules;
	try {
		rules = $('#querybuilder').queryBuilder('getRules');
		console.log("QueryBuilder Rules:", rules);
	} catch (e) {
		console.error("Fehler beim getRules:", e);
		return;
	}

	if (!rules) {
		alert('Ungültige Query (keine rules)');
		return;
	}

	const selected = getSelectedLabels(document.getElementById('querySelection'));
	console.log("Ausgewählte Labels:", selected);

	const url = '/api/get_data_as_table?nodes=' + encodeURIComponent(selected.join(','));
	console.log("Fetch URL:", url);

	fetch(url, {
		method: 'POST',
		headers: {'Content-Type':'application/json','Accept':'application/json'},
		body: JSON.stringify({queryBuilderRules: rules})
	})
		.then(r => {
			console.log("Antwort von /api/get_data_as_table:", r);
			return handleFetchResponse(r);
		})
		.then(data => {
			console.log("Serverdaten erhalten:", data);
			handleServerData(data);
		})
		.catch(err => {
			console.error("Fehler bei runQueryBuilder:", err);
			error('Fehler bei QueryBuilder: ' + (err.message || err));
		});
}

function mapNeoTypeToQB(neoType) {
	console.log("mapNeoTypeToQB:", neoType);
	switch(neoType) {
		case 'String': return 'string';
		case 'Integer': return 'integer';
		case 'Float': return 'double';
		case 'Boolean': return 'boolean';
		default: return 'string';
	}
}

document.addEventListener('DOMContentLoaded', () => {
	console.log("DOMContentLoaded, starte initQueryBuilder");
	initQueryBuilder();
});

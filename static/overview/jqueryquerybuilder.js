"use strict";

function initQueryBuilder() {
	const allowedOperators = ['equal', 'not_equal', 'in', 'not_in', 'less', 'less_or_equal', 'greater', 'greater_or_equal', 'contains', 'begins_with', 'ends_with'];

	fetch('/api/labels')
		.then(r => r.json())
		.then(labels => {
			return Promise.all(labels.map(lbl =>
				fetch('/api/properties?label=' + encodeURIComponent(lbl))
				.then(r => r.json())
				.then(props => ({ label: lbl, props: props }))
			));
		})
		.then(labelInfos => {
			const fields = {};
			labelInfos.forEach(info => {
				info.props.forEach(p => {
					const propName = p.name || p.property;
					if (!propName) {
						console.error("Kein Property-Name für", p, "bei Label", info.label);
						return;
					}
					const key = info.label + '.' + propName;
					fields[key] = {
						id: key,
						label: key,
						type: mapNeoTypeToQB(p.type),
						operators: allowedOperators
					};
				});
			});

			const filters = Object.values(fields);

			if(filters.length) {
				try {
					$('#querybuilder').queryBuilder({
						filters: filters
					});

					restoreQueryBuilderFromUrl();

					// --- ENTER-Event zum Abschicken registrieren ---
					addEnterKeyListener();

				} catch (e) {
					console.error("Fehler beim Init von QueryBuilder:", e);
				}
			} else {
				log("Cannot load jqueryquerybuilder since the list was empty");
			}
		})
		.catch(err => {
			console.error("Fehler in initQueryBuilder:", err);
		});
}

function addEnterKeyListener() {
	$('#querybuilder').on('keydown', 'input, select', function(e) {

		if (e.key === 'Enter') {
			$(e.currentTarget).blur();

			e.preventDefault();

			const rules = $('#querybuilder').queryBuilder('getRules');
			if (!rules || !rules.rules || rules.rules.length === 0) {
				console.warn('Keine gültigen Regeln zum Abschicken.');
				return;
			}

			fetchData();
		}
	});
}

function restoreQueryBuilderFromUrl() {
	var params = new URLSearchParams(window.location.search);
	var qb = params.get('qb');
	if (!qb) {
		fetchData();
		return;
	}

	try {
		var rules = JSON.parse(qb);
		var queryBuilder = $('#querybuilder');
		if (queryBuilder.length && queryBuilder.queryBuilder) {
			try {
				queryBuilder.queryBuilder('setRules', rules);

				fetchData();
			} catch (e) {
				console.warn('Fehler beim Wiederherstellen der QueryBuilder-Regeln', e);
			}
		}
	} catch (e) {
		console.warn('Fehler beim Parsen der QueryBuilder-Regeln aus URL', e);
	}
}


function getQueryBuilderRules() {
	try {
		var queryBuilder = $('#querybuilder');
		if (queryBuilder.length && queryBuilder.queryBuilder) {
			var rules = queryBuilder.queryBuilder('getRules');
			if (!rules || !rules.rules || rules.rules.length === 0) return null;
			return rules;
		}
	} catch (e) {
		console.warn('QueryBuilder noch nicht initialisiert oder keine Regeln vorhanden.');
	}
	return null;
}

function restoreQueryBuilderFromRules(rules) {
	if (!rules) return;
	var queryBuilder = $('#querybuilder');
	if (!queryBuilder.length || !queryBuilder.queryBuilder) {
		log("queryBuilder is empty")
		return;
	}

	try {
		log("trying to set rules:", rules)
		queryBuilder.queryBuilder('setRules', rules);
	} catch (e) {
		console.warn('Fehler beim Wiederherstellen der QueryBuilder-Regeln', e);
	}
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

	fetchData();
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

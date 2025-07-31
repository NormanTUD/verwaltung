if (!("log" in window)) {
    window.log = console.log;
}

function getNamesConfig() {
	var names = {
		room_id: {
			fields: {
				"building_name": {
					name: "Gebäudename",
					type: "select",
					options_url: "/api/get_building_names"
				},
				"room_name": {
					name: "Raumname",
					type: "text"
				}
			},
			label: "Gebäude+Raum",
			url: "/api/get_room_id?building_name={building_name}&room_name={room_name}"
		},
		person_id: {
			fields: {
				"person_name": {
					name: "Person",
					type: "select",
					options_url_id_dict: "/api/get_person_names"
				},
			}
		},
		kostenstelle_id: {
			fields: {
				kostenstelle_id: {
					name: "Objekt",
					type: "select",
					options_url_id_dict: "/api/get_kostenstelle_names"
				}
			},
			label: "Kostenstelle"
		},
		object_id: {
			fields: {
				"object_id": {
					name: "Objekt",
					type: "select",
					options_url_id_dict: "/api/get_object_names"
				}
			},
			label: "Objekt"
		},
		category_id: {
			fields: {
				category_id: {
					name: "Kategorie",
					type: "select",
					options_url_id_dict: "/api/get_category_names"
				}
			},
			label: "Kategorie"
		},
		professorship_id: {
			fields: {
				professorship_id: {
					name: "Professorship",
					type: "select",
					options_url_id_dict: "/api/get_professorship_names"
				}
			},
			label: "Professorship"
		},
		abteilung_id: {
			fields: {
				abteilung_id: {
					name: "Abteilung",
					type: "select",
					options_url_id_dict: "/api/get_abteilung_names"
				}
			},
			label: "Abteilung"
		}
	};

	names['issuer_id'] = names['person_id'];
	names['owner_id'] = names['person_id'];
	names['abteilungsleiter_id'] = names['person_id'];

	return names;
}

function getElementsByName(name) {
	var elements = $('input[name="' + name + '[]"]');
	if (elements.length === 0) {
		elements = $('input[name="' + name + '"]');
	}
	return elements;
}

function createInputField(fieldConfig, fieldName, onOptionsLoaded, default_value) {
	if (fieldConfig.type === "select") {
		var select = $('<select>', {
			class: 'auto_generated_field',
			name: `generated_${fieldName}`,
		});

		var optionsUrl = fieldConfig.options_url || fieldConfig.options_url_id_dict;

		$.get(optionsUrl, function(data) {
			select.append($('<option>', {
				value: "",
				text: "-"
			}));

			if (fieldConfig.options_url_id_dict && typeof data === 'object' && !Array.isArray(data)) {
				for (var id in data) {
					select.append($('<option>', {
						value: id,
						text: data[id],
						selected: id === default_value
					}));
				}
			} else if (Array.isArray(data)) {
				for (var option of data) {
					select.append($('<option>', {
						value: option,
						text: option,
						selected: option === default_value
					}));
				}
			}

			if (typeof onOptionsLoaded === "function") {
				onOptionsLoaded(select);
			}
		}).fail(function() {
			log("Fehler beim Laden der Optionen für " + fieldName);
			if (typeof onOptionsLoaded === "function") {
				onOptionsLoaded(select);
			}
		});

		return select;
	} else {
		return $('<input>', {
			type: 'text',
			class: 'auto_generated_field',
			name: `generated_{fieldName}`,
			placeholder: fieldConfig.name
		});
	}
}

function updateHiddenFieldValue(config, hiddenElement, form, triggeredBy = null) {
	var params = {};

	if(triggeredBy) {
		var $triggeredBy = $(triggeredBy);

		var $children = $triggeredBy.parent().find(".auto_generated_field")

		$children.each(function () {
			var $field = $(this);
			var name = $field.attr("name");

			// Sicherheit: Nur verarbeiten, wenn "name" vorhanden ist
			if (typeof name !== "undefined" && name !== null) {
				var val = $field.val();
				params[name] = val;
			}
		});
	} else {
		for (var key in config.fields) {
			var val = form.find('[name="' + key + '"]').val();
			params[key] = val;
		}
	}

	//log("updating ", hiddenElement, " to ", params, ", triggered by: ", triggeredBy);

	if (config.url) {
		var failed = false;
		var newUrl = config.url.replace(/\{(\w+)\}/g, function(match, p1) {
			if(params[p1]) {
				return encodeURIComponent(params[p1]);
			} else {
				failed = true;
				return '';
			}
		});

		if(!failed) {
			$.get(newUrl, function(data) {
				console.log("AJAX response data:", data);
				if (data && data.room_id) {
					console.log("Setting hidden field to room_id:", data.room_id);
					hiddenElement.val(data.room_id);
				} else if (data && data.person_id) {
					console.log("Setting hidden field to person_id:", data.person_id);
					hiddenElement.val(data.person_id);
				} else {
					console.log("No valid ID found in response, clearing hidden field");
					hiddenElement.val('');
				}
			}).fail(function() {
				console.log("AJAX request failed");
				hiddenElement.val('');
			});
		}
	}
}

function autoUpdate(element_name, update_typ, update_id, new_val) {
	try {
		var baseUrl = "/api/auto_update/";
		var fullUrl = baseUrl + encodeURIComponent(update_typ)
			+ "?name=" + encodeURIComponent(element_name)
			+ "&id=" + encodeURIComponent(update_id)
			+ "&val=" + encodeURIComponent(new_val);

		fetch(fullUrl, {
			method: "GET"
		})
			.then(function(response) {
				if (!response.ok) {
					throw new Error("HTTP error, status = " + response.status);
				}
				return response.json();
			})
			.then(function(data) {
				success(data.message, "Erfolgreich");
			})
			.catch(function(error) {
				log("Fetch error:", error);

				var msg = "Fehler beim Senden der Anfrage: " + error.message;
				error(msg, "Fehler");
			});
	} catch (e) {
		log("Unexpected error:", e);

		var msg = "Unerwarteter Fehler: " + e.message;
		error(msg, "Fehler");
	}
}

function replaceFieldsForElement(element, name, config) {
	var $element = $(element);
	var original_value = $(element).attr("value");
	var update_info = $element.data("update_info");
	var element_name = $element.attr("name");

	if(Object.keys(config).includes("label")) {
		var $parentLabel = $element.parent().find("label");
		if ($parentLabel.length > 0) {
			$parentLabel.text(config.label);
		}
	}

	if (!$element.is(":visible")) {
		log("Element is not visible, skipping field replacement.");
		return;
	}

	var form = $element.closest('form');

	var inputs = [];
	var i = 0;

	function onInputChange() {
		updateHiddenFieldValue(config, $element, form);
	}

	for (let fieldName in config.fields) {
		let fieldConfig = config.fields[fieldName];

		let input = createInputField(fieldConfig, fieldName, function(selectElement) {
			onInputChange();
		}, original_value);

		$(input).on("change", function (e) {
			var target = e.currentTarget;
			var new_val = $(target).val();
			if(update_info) {
				var update_typ = update_info.slice(0, update_info.lastIndexOf("_"))
				var update_id = update_info.slice(update_info.lastIndexOf("_") + 1);

				autoUpdate(element_name, update_typ, update_id, new_val)
			}

			$element.val(new_val);
		});

		$element.before(input);
		inputs.push(input);

		input.on('blur', (function(hiddenElement) {
			return function() {
				var form = $(this).closest('form');
				updateHiddenFieldValue(config, hiddenElement, form, this);
			};
		})($element));



		i++;
	}

	$element.hide();

	// Direkt nach Erzeugung einmal initial updaten (für Textfelder oder Select mit sofort ausgewähltem Wert)
	onInputChange();
}

function replace_id_fields_with_proper_fields() {
	var names = getNamesConfig();

	for (let name of Object.keys(names)) {
		var elements = getElementsByName(name);

		// Wichtig: jedes element separat mit eigenem Kontext behandeln
		for (let k = 0; k < elements.length; k++) {
			let element = $(elements[k]);
			let config = names[name];
			replaceFieldsForElement(element, name, config);
		}
	}

	applyInvertFilterToElements(theme);
}


$( document ).ready(function() {
	replace_id_fields_with_proper_fields();

	$('.module-toggle').on('change', function () {
		var target = $(this).data('target');
		if ($(this).is(':checked')) {
			$(target).slideDown(200);
		} else {
			$(target).slideUp(200);
		}
	});

	// Initialstatus erzwingen
	$('.module-toggle').each(function () {
		var target = $(this).data('target');
		if (!$(this).is(':checked')) {
			$(target).hide();
		}
	});
});

(function() {
	let originalTableData = new WeakMap(); 
	const arrows = { none: '↔', asc: '↑', desc: '↓' };

	function compare(a, b, type) {
		a = a.trim(); b = b.trim();
		if (type === 'num') {
			let na = parseFloat(a.replace(',', '.'));
			let nb = parseFloat(b.replace(',', '.'));
			return na - nb;
		} else {
			return a.localeCompare(b);
		}
	}

	function detectType(val) {
		return /^-?\d+([.,]\d+)?$/.test(val.trim()) ? 'num' : 'text';
	}

	function getCellValue(row, idx) {
		const td = $(row).children('td').eq(idx);
		const input = td.find('input[type=text], select').first();
		return input.length ? input.val() || input.find('option:selected').text() : td.text();
	}

	function applySorting($table, colIndex, direction) {
		const $tbody = $table.find('tbody');
		let rows = originalTableData.get($table)['originalRows'];

		if (direction === 'asc' || direction === 'desc') {
			const type = detectType(getCellValue(rows[0], colIndex));
			rows = rows.slice().sort((a, b) => {
				const va = getCellValue(a, colIndex);
				const vb = getCellValue(b, colIndex);
				return direction === 'asc' ? compare(va, vb, type) : compare(vb, va, type);
			});
		}

		$tbody.empty().append(rows);
		// Update current sorted rows so subsequent sorts always sort original data
		originalTableData.get($table)['currentRows'] = rows;
	}

	function initSorting() {
		$('table').each(function() {
			const $table = $(this);
			const $thead = $table.find('thead');
			const $tbody = $table.find('tbody');
			if (!$thead.length || !$tbody.length) return;

			const $headers = $thead.find('th');
			if (!$headers.length) return;

			let rows = $tbody.children('tr').get();
			if (!rows.length) return;

			// Speichere Original Reihenfolge komplett
			originalTableData.set($table, { originalRows: rows, currentRows: rows });

			$headers.each(function(i) {
				const $th = $(this);
				if ($th.text().trim() === '') return;

				$th.css('cursor', 'pointer');

				// Pfeil-Spanne nur hinzufügen, nicht Text ersetzen
				if ($th.find('.sort-arrow').length === 0) {
					$th.append(' ').append($('<span>').addClass('sort-arrow').text(arrows.none));
				}

				let state = 'none';

				$th.off('click').on('click', function() {
					try {
						// Alle Pfeile auf neutral setzen
						$headers.find('.sort-arrow').text(arrows.none);

						// Status wechseln
						if (state === 'none') {
							state = 'asc';
						} else if (state === 'asc') {
							state = 'desc';
						} else {
							state = 'none';
						}

						// Pfeil aktualisieren
						$th.find('.sort-arrow').text(arrows[state]);

						applySorting($table, i, state);
					} catch (e) {
						error("Sorting error: " + e.message);
					}
				});
			});
		});
	}

	$(document).ready(function() {
		try {
			initSorting();
		} catch (e) {
			error("Fehler beim Initialisieren der Sortierung: " + e.message);
		}
	});
})();

if (!("log" in window)) {
    window.log = console.log;
}

function getNamesConfig() {
	var names = {
		raum_id: {
			fields: {
				"gebäudename": {
					name: "Gebäudename",
					type: "select",
					options_url: "/api/get_building_names"
				},
				"raumname": {
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
		lager_id: {
			fields: {
				"lager_id": {
					name: "Lager",
					type: "select",
					options_url_id_dict: "/api/get_lager_names"
				}
			},
			label: "Lager"
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
		professur_id: {
			fields: {
				professur_id: {
					name: "Professur",
					type: "select",
					options_url_id_dict: "/api/get_professur_names"
				}
			},
			label: "Professur"
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



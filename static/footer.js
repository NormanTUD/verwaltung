if (!("log" in window)) {
    window.log = console.log;
}

var backlink = $("#backLink");
if (backlink.length) {
	backlink = $("#backLink")[0];
	backlink.addEventListener('click', function(event) {
		event.preventDefault(); // href erstmal verhindern

		if (window.history.length > 1) {
			history.back();
		} else {
			// Keine History, dann href öffnen
			window.location.href = this.href;
		}
	});
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
		}
	};

	names['issuer_id'] = names['person_id'];
	names['owner_id'] = names['person_id'];

	return names;
}

function getElementsByName(name) {
	var elements = $('input[name="' + name + '[]"]');
	if (elements.length === 0) {
		elements = $('input[name="' + name + '"]');
	}
	return elements;
}

function createInputField(fieldConfig, fieldName, onOptionsLoaded) {
	if (fieldConfig.type === "select") {
		var select = $('<select>', {
			class: 'auto_generated_field',
			name: fieldName
		});

		var optionsUrl = fieldConfig.options_url || fieldConfig.options_url_id_dict;

		$.get(optionsUrl, function(data) {
			if (fieldConfig.options_url_id_dict && typeof data === 'object' && !Array.isArray(data)) {
				for (var id in data) {
					select.append($('<option>', {
						value: id,
						text: data[id]
					}));
				}
			} else if (Array.isArray(data)) {
				for (var option of data) {
					select.append($('<option>', {
						value: option,
						text: option
					}));
				}
			}

			// Automatisch ersten Wert auswählen und callback ausführen
			if (select.children().length > 0) {
				select.val(select.children().first().val());
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
			name: fieldName,
			placeholder: fieldConfig.name
		});
	}
}

function updateHiddenFieldValue(config, hiddenElement, form) {
	var params = {};

	for (var key in config.fields) {
		var val = form.find('[name="' + key + '"]').val();
		params[key] = val;
	}

	if (config.url) {
		var newUrl = config.url.replace(/\{(\w+)\}/g, function(match, p1) {
			return encodeURIComponent(params[p1] || '');
		});

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
	} else {
		var firstField = Object.keys(config.fields)[0];
		hiddenElement.val(params[firstField] || '');
	}
}


function replaceFieldsForElement(element, name, config) {
	var $element = $(element);
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
		});

		if (i === 0) {
			$element.before($("<br>"));
		}

		$element.before(input);
		inputs.push(input);

		input.on('input change', (function(hiddenElement) {
			return function() {
				var form = $(this).closest('form');
				console.log("Event triggered on input:", this);
				console.log("Closest form:", form);
				console.log("Updating hidden element:", hiddenElement);
				updateHiddenFieldValue(config, hiddenElement, form);
			};
		})($element));



		i++;
	}

	//$element.hide();

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
}


$( document ).ready(function() {
	replace_id_fields_with_proper_fields();
});

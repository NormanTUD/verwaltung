if (!("log" in window)) {
    window.log = console.log;
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
		var input = $('<input>', {
			type: 'text',
			class: 'auto_generated_field autocomplete_input',
			name: `generated_${fieldName}`,
			placeholder: fieldConfig.name,
			autocomplete: 'off',
			val: default_value || ""
		});

		var wrapper = $('<div>', { class: 'autocomplete_wrapper', css: { position: 'relative' } });
		var suggestionsBox = $('<div>', {
			class: 'autocomplete_suggestions',
			css: {
				position: 'absolute',
				top: '100%',
				left: 0,
				right: 0,
				zIndex: 9999,
				backgroundColor: '#fff',
				border: '1px solid #ccc',
				borderTop: 'none',
				display: 'none',
				maxHeight: '150px',
				overflowY: 'auto'
			}
		});

		wrapper.append(input).append(suggestionsBox);

		var optionsUrl = fieldConfig.options_url || fieldConfig.options_url_id_dict;

		$.get(optionsUrl, function(data) {
			var options = [];

			if (fieldConfig.options_url_id_dict && typeof data === 'object' && !Array.isArray(data)) {
				options = Object.values(data);
			} else if (Array.isArray(data)) {
				options = data;
			} else if (typeof data === 'object' && data !== null && Object.values(data).every(v => typeof v === 'string')) {
				options = Object.values(data);
			}

			input.val(default_value || "");

			input.on('input', function() {
				var value = input.val().toLowerCase();
				suggestionsBox.empty();

				if (!value) {
					suggestionsBox.hide();
					return;
				}

				var filtered = options.filter(function(opt) {
					return opt.toLowerCase().includes(value);
				});

				if (filtered.length === 0) {
					suggestionsBox.hide();
					return;
				}

				for (var opt of filtered) {
					var item = $('<div>', {
						text: opt,
						css: {
							padding: '4px 8px',
							cursor: 'pointer'
						},
						mouseover: function() {
							$(this).css('background-color', '#eee');
						},
						mouseout: function() {
							$(this).css('background-color', '#fff');
						},
						click: function() {
							input.val($(this).text());
							suggestionsBox.hide();
						}
					});
					suggestionsBox.append(item);
				}

				suggestionsBox.show();
			});

			// Klick außerhalb -> Autocomplete schließen
			$(document).on('click', function(event) {
				if (!wrapper[0].contains(event.target)) {
					suggestionsBox.hide();
				}
			});

			if (typeof onOptionsLoaded === "function") {
				onOptionsLoaded(input);
			}
		}).fail(function() {
			log("Fehler beim Laden der Optionen für " + fieldName);
			if (typeof onOptionsLoaded === "function") {
				onOptionsLoaded(input);
			}
		});

		return wrapper;
	} else {
		return $('<input>', {
			type: 'text',
			class: 'auto_generated_field',
			name: `generated_${fieldName}`,
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
				if (data && data.raum_id) {
					console.log("Setting hidden field to raum_id:", data.raum_id);
					hiddenElement.val(data.raum_id);
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
	var original_value = $element.attr("value");
	var update_info = $element.data("update_info");
	var element_name = $element.attr("name");

	if (Object.keys(config).includes("label")) {
		var $parentLabel = $element.parent().find("label");
		if ($parentLabel.length > 0) {
			$parentLabel.text(config.label);
		}
	}

	if (!$element.is(":visible")) return;

	var form = $element.closest('form');
	var inputs = [];

	function onInputChange() {
		updateHiddenFieldValue(config, $element, form);
	}

	// Jetzt korrekt über das Array von Feldern iterieren
	for (let fieldDef of config.fields) {
		// Jeder fieldDef ist ein Objekt mit genau einem Key
		let fieldName = Object.keys(fieldDef)[0];
		let fieldConfig = fieldDef[fieldName];

		let input = createInputField(fieldConfig, fieldName, function() {
			onInputChange();
		}, original_value);

		$(input).on("change", function (e) {
			var new_val = $(e.currentTarget).val();
			if (update_info) {
				var update_typ = update_info.slice(0, update_info.lastIndexOf("_"));
				var update_id = update_info.slice(update_info.lastIndexOf("_") + 1);
				autoUpdate(element_name, update_typ, update_id, new_val);
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
	}

	$element.hide();
	onInputChange();
}

async function replace_id_fields_with_proper_fields() {
	for (let name of Object.keys(replace_names)) {
		var elementsExact = getElementsByName(name);
		var elementsArray = getElementsByName(name + "[]");

		var elements = Array.from(elementsExact).concat(Array.from(elementsArray));

		for (let k = 0; k < elements.length; k++) {
			let element = $(elements[k]);
			let config = replace_names[name];
			replaceFieldsForElement(element, name, config);
		}
	}

	applyInvertFilterToElements(theme);
}

$( document ).ready(function() {
	const path = location.pathname;

	replace_id_fields_with_proper_fields().then(() => {
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
});



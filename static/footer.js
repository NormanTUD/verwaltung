const log = console.log;

var backlink = document.querySelector('.backlink');
if (backlink) {
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
            },
            label: "Person",
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

function createInputField(fieldConfig, fieldName) {
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
        }).fail(function() {
            log("Fehler beim Laden der Optionen für " + fieldName);
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

function updateHiddenFieldValue(config, element, urlTemplate) {
    var form = element.closest('form');
    var params = {};

    for (var key in config.fields) {
        var val = form.find('[name="' + key + '"]').val();
        params[key] = val;
    }

    if (urlTemplate) {
        var newUrl = urlTemplate.replace(/\{(\w+)\}/g, function(match, p1) {
            return encodeURIComponent(params[p1] || '');
        });

        log(newUrl);

        $.get(newUrl, function(data) {
            log(data);
            if (data && data.room_id) {
                element.val(data.room_id);
            }
        }).fail(function() {
            log("Fehler beim Abrufen der Raum-ID");
            element.val('');
        });
    } else {
        // For cases without a urlTemplate, set the hidden field directly from the current input
        // Use first field in fields or a suitable fallback?
        var firstField = Object.keys(config.fields)[0];
        element.val(params[firstField] || '');
    }
}

function replaceFieldsForElement(element, name, config) {
    $(element).parent().find("label").text(config.label);

    if (!$(element).is(":visible")) {
        log("Element is not visible, skipping field replacement.");
        return;
    }

    var i = 0;
    for (var fieldName in config.fields) {
        var fieldConfig = config.fields[fieldName];
        var input = createInputField(fieldConfig, fieldName);

        if (i === 0) {
            $(element).before($("<br>"));
        }

        $(element).before(input);

        input.on('input change', function() {
            updateHiddenFieldValue(config, $(element), config.url);
        });

        i++;
    }

    $(element).hide();
}

function replace_id_fields_with_proper_fields() {
    var names = getNamesConfig();

    for (var name of Object.keys(names)) {
        var elements = getElementsByName(name);

        for (var k = 0; k < elements.length; k++) {
            var element = $(elements[k]);
            var config = names[name];
            replaceFieldsForElement(element, name, config);
        }
    }
}

$( document ).ready(function() {
    replace_id_fields_with_proper_fields();
});
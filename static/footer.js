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

function replace_id_fields_with_proper_fields() {
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
            // URL für Select: /api/get_person_names
        }
    };
    /*
    names['issuer_id'] = names['person_id'];
    names['owner_id'] = names['person_id'];
    */

    for (var name of Object.keys(names)) {
        var elements = $('input[name="' + name + '[]"]');

        if(elements.length === 0) {
            elements = $('input[name="' + name + '"]');
        }

        for (var k = 0; k < elements.length; k++) {
            var element = $(elements[k]);
            var config = names[name];
            var url = config.url;

            $(element).parent().find("label").text(config.label);

            if (!$(element).is(":visible")) {
                log("Element is not visible, skipping field replacement.");
                continue;
            }

            var i = 0;

            for (var field in config.fields) {
                var fieldConfig = config.fields[field];
                var input;

                if (fieldConfig.type === "select") {
                    input = $('<select>', {
                        class: 'auto_generated_field',
                        name: field
                    });

                    // ⬇️ Lokale Kopie für korrekte Referenzierung im Callback
                    (function(selectElement, optionsUrl, fieldName) {
                        $.get(optionsUrl, function(data) {
                            if (Array.isArray(data)) {
                                for (var option of data) {
                                    selectElement.append($('<option>', {
                                        value: option,
                                        text: option
                                    }));
                                }
                            }
                        }).fail(function() {
                            log("Fehler beim Laden der Optionen für " + fieldName);
                        });
                    })(input, fieldConfig.options_url, field);  // <-- lokale Kopie
                } else {
                    input = $('<input>', {
                        type: 'text',
                        class: 'auto_generated_field',
                        name: field,
                        placeholder: fieldConfig.name
                    });
                }

                if (i === 0) {
                    element.before($("<br>"));
                }

                element.before(input);

                // Event-Handler: auch für 'change', weil select kein 'input' auslöst
                input.on('input change', function() {
                    var params = {};
                    var form = $(this).closest('form');

                    for (var key in config.fields) {
                        var val = form.find('[name="' + key + '"]').val();
                        params[key] = val;
                    }

                    var newUrl = url.replace(/\{(\w+)\}/g, function(match, p1) {
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
                });

                i++;
            }

            $(element).hide();
        }
    }
}

$( document ).ready(function() {
    replace_id_fields_with_proper_fields();
});
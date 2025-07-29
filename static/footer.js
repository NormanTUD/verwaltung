const log = console.log;

document.getElementById('backLink').addEventListener('click', function(event) {
    event.preventDefault(); // href erstmal verhindern

    if (window.history.length > 1) {
        history.back();
    } else {
        // Keine History, dann href öffnen
        window.location.href = this.href;
    }
});

function replace_id_fields_with_proper_fields () {
    var names = {
        room_id: {
            fields: {
                "building_name": "Gebäudename",
                "room_name": "Raumname"
            },
            label: "Gebäude+Raum",
            url: "/api/get_room_id?building_name={building_name}&room_name={room_name}"
        }
    };

    for (var name of Object.keys(names)) {
        var element = $('input[name="' + name + '[]"]');
        var url = names[name].url;

        log(element);

        $(element).parent().find("label").text(names[name].label);

        if(!$(element).is(":visible")) {
            log("Element is not visible, skipping field replacement.");
            continue; // Element is not visible, skip this field
        }

        var i = 0;

        for (var field in names[name].fields) {
            var fieldName = names[name].fields[field];
            var input = $('<input>', {
                type: 'text',
                class: 'auto_generated_field',
                name: field,
                placeholder: fieldName
            });

            if (i === 0) {
                element.before($("<br>"));
            }

            element.before(input);

            input.on('input', function() {
                var params = {};

                for (var field of Object.keys(names[name].fields)) {
                    var value = $(this).closest('form').find('input[name="' + field + '"]').val();
                    params[field] = value;
                }
                var queryString = $.param(params);
                var newUrl = url.replace(/\{(\w+)\}/g, function(match, p1) {
                    return params[p1] || '';
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
                }
                );
            });

            $(element).hide();

            i++;
        }
    }
}

$( document ).ready(function() {
    replace_id_fields_with_proper_fields();
});
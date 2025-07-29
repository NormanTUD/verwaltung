<<<<<<< HEAD
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
        console.log(name);
        var element = $('input[name="' + name + '[]"]');
        var url = names[name].url;
        console.log(url);

        log(element);

        for (var field in names[name].fields) {
            var fieldName = names[name].fields[field];
            var input = $('<input>', {
                type: 'text',
                name: field,
                placeholder: fieldName
            });

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
        }
    }
}

$( document ).ready(function() {
    replace_id_fields_with_proper_fields();
});
=======
var backlink = document.getElementById('backLink');

if(backlink) {
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
>>>>>>> 9db132359a7c426fffa9cad2a306a1a2103ef14d

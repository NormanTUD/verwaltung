const log = console.log;

// Funktion, die prüft, ob mindestens ein Feld in der neuen Zeile gefüllt ist
function checkNewEntryInputs() {
        const inputs = $(".new-entry input, .new-entry select");
        let isAnyFilled = false;

        inputs.each(function() {
                const tag = this.tagName.toUpperCase();
                let value = $(this).val();

                if (tag === "SELECT") {
                        // Für Mehrfachauswahl-Selects
                        if (Array.isArray(value) && value.length > 0) {
                                isAnyFilled = true;
                                return false;
                        }

                        // Für normale Selects: Index > 0 oder gültiger nicht-leerer Wert
                        if (this.selectedIndex > 0) {
                                isAnyFilled = true;
                                return false;
                        }

                        // Alternativ: Wenn value gesetzt ist (und keine leere Option existiert)
                        if (value !== "" && value !== null && value !== undefined) {
                                isAnyFilled = true;
                                return false;
                        }

                } else if (tag === "INPUT") {
                        if (typeof value === "string") {
                                value = value.trim();
                        }

                        if (value !== "" && value !== null && value !== undefined) {
                                isAnyFilled = true;
                                return false;
                        }
                }
        });

        $(".save-new").prop("disabled", !isAnyFilled);
}

// Beim Laden der Seite Button deaktivieren
$(document).ready(function() {
	// Button per default deaktivieren
	$(".save-new").prop("disabled", true);

	$(".new-entry input, .new-entry select").on("input change", function() {
		checkNewEntryInputs();
	});

	checkNewEntryInputs();
});

// Bestehender Update-Code für vorhandene Einträge (unverändert)
$(".cell-input").filter(function() {
	return $(this).closest(".new-entry").length === 0;
}).on("change", function() {
	const name = $(this).attr("name");
	const value = $(this).val();
	$.post("/update/{{ table_name }}", { name, value }, function(resp) {
		if (!resp.success) {
			error("Fehler beim Updaten: " + resp.error);
		} else {
			success("Eintrag geupdatet");
		}
	}, "json").fail(function() {
		error("Netzwerkfehler beim Updaten");
	});
});

// Speichern neuer Eintrag
$(".save-new").on("click", function() {
	const data = {};
	$(".new-entry input, .new-entry select").each(function() {
		data[$(this).attr("name")] = $(this).val();
	});
	$.post("/add/{{ table_name }}", data, function(resp) {
		if (!resp.success) {
			error("Fehler beim Speichern: " + resp.error);
		} else {
			success("Eintrag gespeichert");
			location.reload();
		}
	}, "json").fail(function() {
		error("Netzwerkfehler beim Speichern");
	});
});

// Löschen Eintrag
$(".delete-entry").on("click", function() {
	const $row = $(this).closest("tr");
	const id = $row.data("id");

	if (id === null || id === undefined) {
		error(`Datensatz-ID nicht gefunden: ${id}.`);
		return;
	}

	if (!confirm("Soll dieser Eintrag wirklich gelöscht werden?")) {
		return;
	}

	$.ajax({
		url: "/delete/{{ table_name }}",
		method: "POST",
		contentType: "application/json",
		data: JSON.stringify({ id: id }),
		dataType: "json"
	}).done(function(resp) {
		if (!resp.success) {
			error("Fehler beim Löschen: " + resp.error);
		} else {
			success("Eintrag gelöscht");
			$row.remove();
		}
	}).fail(function() {
		error("Netzwerkfehler beim Löschen");
	});
});

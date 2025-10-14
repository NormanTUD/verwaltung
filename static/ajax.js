function open_link(link) {
	try {
		if (typeof link !== "string" || link.trim() === "") {
			console.error("open_link(): Ungültiger Link übergeben.");
			return;
		}

		var mainContent = $("#main_content");
		if (mainContent.length === 0) {
			console.error("open_link(): Element #main_content nicht gefunden.");
			return;
		}

		var spinnerHtml = '' +
			'<div id="ajax_spinner" style="display:flex;align-items:center;justify-content:center;height:100%;min-height:100px;">' +
			'  <div style="border:6px solid #f3f3f3;border-top:6px solid #3498db;border-radius:50%;width:40px;height:40px;animation:spin 1s linear infinite;"></div>' +
			'  <style>@keyframes spin { 0% { transform: rotate(0deg); } 100% { transform: rotate(360deg); } }</style>' +
			'</div>';

		mainContent.html(spinnerHtml);

		$.ajax({
			url: link,
			type: "GET",
			cache: false,
			dataType: "html",
			success: function (data, textStatus, jqXHR) {
				try {
					if (typeof data !== "string" || data.trim() === "") {
						console.warn("open_link(): Keine oder leere Antwort empfangen.");
						mainContent.html("<div style='color:red;padding:10px;'>Fehler: Leere Antwort.</div>");
						return;
					}

					// 1️⃣ HTML-Inhalt parsen
					var parsed = $("<div>").html(data);

					// 2️⃣ Skripte extrahieren
					var scripts = parsed.find("script");

					// 3️⃣ Inhalt ohne Skripte in mainContent einsetzen
					mainContent.html(parsed);

					// 4️⃣ Skripte nacheinander ausführen
					scripts.each(function () {
						var script = $(this);
						var src = script.attr("src");
						var code = script.html();

						try {
							if (src) {
								// Externe Datei laden
								$.ajax({
									url: src,
									dataType: "script",
									cache: true,
									async: false, // Reihenfolge beibehalten
									error: function (xhr, status, err) {
										console.error("Fehler beim Laden von Script:", src, status, err);
									}
								});
							} else if (code.trim() !== "") {
								// Inline-Skript ausführen
								$.globalEval(code);
							}
						} catch (scriptError) {
							console.error("Fehler beim Ausführen eines Skripts:", scriptError);
						}
					});

					// 5️⃣ URL in History setzen
					if (window.history && window.history.pushState) {
						window.history.pushState({ ajaxLoaded: true, url: link }, "", link);

						try {
							get_data_overview();
						} catch (e) {
							//
						}
					} else {
						console.warn("open_link(): Browser unterstützt history.pushState nicht.");
					}

				} catch (innerError) {
					console.error("open_link(): Fehler beim Schreiben des Inhalts:", innerError);
					mainContent.html("<div style='color:red;padding:10px;'>Fehler beim Verarbeiten des Inhalts.</div>");
				}
			},
			error: function (jqXHR, textStatus, errorThrown) {
				console.error("open_link(): AJAX-Fehler:", textStatus, errorThrown);
				var errorMsg = "<div style='color:red;padding:10px;'>Fehler beim Laden der Seite:<br>" +
					$("<div>").text(textStatus + ": " + errorThrown).html() + "</div>";
				mainContent.html(errorMsg);
			}
		});

	} catch (error) {
		console.error("open_link(): Allgemeiner Fehler:", error);
		var fallback = "<div style='color:red;padding:10px;'>Ein unerwarteter Fehler ist aufgetreten.</div>";
		$("#main_content").html(fallback);
	}
}

// Event-Handler für "Zurück"-Navigation im Browser
$(window).on("popstate", function (event) {
	try {
		var state = event.originalEvent.state;
		if (state && state.ajaxLoaded && typeof state.url === "string") {
			console.log("popstate: Lade vorherige Seite:", state.url);
			open_link(state.url);
		}
	} catch (error) {
		console.error("popstate(): Fehler beim Laden vorheriger Seite:", error);
	}
});

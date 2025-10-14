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

		// Spinner HTML & CSS inline
		var spinnerHtml = '' +
			'<div id="ajax_spinner" style="display:flex;align-items:center;justify-content:center;height:100%;min-height:100px;">' +
			'  <div style="border:6px solid #f3f3f3;border-top:6px solid #3498db;border-radius:50%;width:40px;height:40px;animation:spin 1s linear infinite;"></div>' +
			'  <style>@keyframes spin { 0% { transform: rotate(0deg); } 100% { transform: rotate(360deg); } }</style>' +
			'</div>';

		// Spinner anzeigen
		mainContent.html(spinnerHtml);

		// AJAX GET
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

					// Inhalt einsetzen
					mainContent.html(data);

					// URL ändern, ohne neu zu laden
					if (window.history && window.history.pushState) {
						window.history.pushState({ ajaxLoaded: true, url: link }, "", link);
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
			},
			complete: function (jqXHR, textStatus) {
				console.log("open_link(): AJAX abgeschlossen mit Status:", textStatus);
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

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

					// 2️⃣ Skripte extrahieren und entfernen
					var scripts = parsed.find("script");
					scripts.remove();

					// 3️⃣ Nur den Inhalt einsetzen (ohne Skripte)
					mainContent.html(parsed.html());

					// 4️⃣ Skripte in Reihenfolge asynchron ausführen
					executeScriptsSequentially(scripts).then(function () {
						// 5️⃣ URL in History setzen
						if (window.history && window.history.pushState) {
							window.history.pushState({ ajaxLoaded: true, url: link }, "", link);
						}

						try {
							var url = new URL(window.location.href);
							var path = url.pathname.replace(/\/+$/, "");

							if (path === "/overview") {
								get_data_overview();
							} else if (path === "/query_overview") {
								loadQueries();
							} else if (path === "/upload") {
								load_mapping();
							}
						} catch (err) {
							console.error("open_link(): Fehler in URL-Verarbeitung:", err);
						}
					});

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
		$("#main_content").html("<div style='color:red;padding:10px;'>Ein unerwarteter Fehler ist aufgetreten.</div>");
	}
}

// Hilfsfunktion für serielles Scriptladen
function executeScriptsSequentially(scripts) {
	return new Promise(function (resolve, reject) {
		var index = 0;

		function next() {
			if (index >= scripts.length) {
				resolve();
				return;
			}

			var script = $(scripts[index++]);
			var src = script.attr("src");
			var code = script.html();

			try {
				if (src) {
					$.getScript(src)
						.done(next)
						.fail(function (xhr, status, err) {
							console.error("Fehler beim Laden von Script:", src, status, err);
							next();
						});
				} else if (code.trim() !== "") {
					$.globalEval(code);
					next();
				} else {
					next();
				}
			} catch (err) {
				console.error("executeScriptsSequentially(): Fehler:", err);
				next();
			}
		}

		next();
	});
}

// Browser-Zurück-Event
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

"use strict";

function insertDefaultData(preset) {
	const textarea = document.getElementById('data');
	let data = '';

	if (preset === 'persons') {
		data = `vorname,nachname,titel,geburtsjahr,stadt
Anna,Müller,Dr.,1984,Berlin
Bernd,Schmidt,,1972,Hamburg
Clara,Fischer,Prof.,1990,München
David,Weber,,1987,Stuttgart
Eva,Wagner,Dr.,1995,Leipzig
Felix,Becker,,1980,Köln
Greta,Hofmann,Prof.,1993,Dresden
Hans,Klein,,1975,Hannover
Ines,Schäfer,Dr.,1988,Bremen
Jonas,Bauer,,1992,Frankfurt`;
	} else if (preset === 'customers') {
		data = `vorname,nachname,kundennummer,email,produkt
Anna,Müller,1001,anna.mueller@example.com,Laptop
Bernd,Schmidt,1002,bernd.schmidt@example.com,Smartphone
Clara,Fischer,1003,clara.fischer@example.com,Tablet
David,Weber,1004,david.weber@example.com,Monitor
Eva,Wagner,1005,eva.wagner@example.com,Headphones
Paul,Krüger,1006,paul.krueger@example.com,Smartwatch
Mona,Lange,1007,mona.lange@example.com,Kamera
Tom,Scholz,1008,tom.scholz@example.com,Drucker
Nina,Krause,1009,nina.krause@example.com,Router
Oliver,Seidel,1010,oliver.seidel@example.com,Beamer`;
	} else if (preset === 'orders') {
		data = `bestellnr,kundennummer,datum,betrag,status
5001,1001,2023-01-15,1200,bezahlt
5002,1002,2023-02-03,800,offen
5003,1003,2023-02-14,300,bezahlt
5004,1004,2023-03-20,220,storniert
5005,1005,2023-03-25,150,bezahlt
5006,1006,2023-04-01,950,offen
5007,1007,2023-04-18,400,bezahlt
5008,1008,2023-05-07,600,bezahlt
5009,1009,2023-05-10,130,offen
5010,1010,2023-05-15,2000,bezahlt`;
	} else if (preset === 'shipments') {
		data = `versandnr,bestellnr,datum,versandart,tracking
9001,5001,2023-01-16,DHL,DE123456
9002,5002,2023-02-05,Hermes,HE654321
9003,5003,2023-02-16,DHL,DE987654
9004,5004,2023-03-21,UPS,UP111111
9005,5005,2023-03-26,DHL,DE222222
9006,5006,2023-04-03,Hermes,HE333333
9007,5007,2023-04-19,UPS,UP444444
9008,5008,2023-05-08,DHL,DE555555
9009,5009,2023-05-12,Hermes,HE666666
9010,5010,2023-05-16,UPS,UP777777`;
	}

	textarea.value = data.trim();
}

function add_import_handlers() {
	// Formular-Abfang
	$(document).on("submit", "#upload_form", function (e) {
		try {
			e.preventDefault();

			var form = $(this);
			var actionUrl = form.attr("action");
			var method = form.attr("method") || "POST";

			if (typeof actionUrl !== "string" || actionUrl.trim() === "") {
				console.error("AJAX-Upload: Ungültige action-URL.");
				return;
			}

			var mainContent = $("#main_content");
			if (mainContent.length === 0) {
				console.error("AJAX-Upload: #main_content nicht gefunden.");
				return;
			}

			var formData = form.serialize();

			// Spinner anzeigen
			var spinnerHtml = '' +
				'<div id="ajax_spinner" style="display:flex;align-items:center;justify-content:center;height:100%;min-height:100px;">' +
				'  <div style="border:6px solid #f3f3f3;border-top:6px solid #3498db;border-radius:50%;width:40px;height:40px;animation:spin 1s linear infinite;"></div>' +
				'  <style>@keyframes spin { 0% { transform: rotate(0deg); } 100% { transform: rotate(360deg); } }</style>' +
				'</div>';
			mainContent.html(spinnerHtml);

			// AJAX-POST durchführen
			$.ajax({
				url: actionUrl,
				type: method.toUpperCase(),
				data: formData,
				dataType: "html",
				cache: false,
				success: function (data, textStatus, jqXHR) {
					try {
						if (typeof data !== "string" || data.trim() === "") {
							console.warn("AJAX-Upload: Leere Antwort empfangen.");
							mainContent.html("<div style='color:red;padding:10px;'>Fehler: Leere Antwort.</div>");
							return;
						}

						// HTML-Inhalt parsen
						var parsed = $("<div>").html(data);
						var scripts = parsed.find("script");

						mainContent.html(parsed);

						// Skripte ausführen
						scripts.each(function () {
							var script = $(this);
							var src = script.attr("src");
							var code = script.html();

							try {
								if (src) {
									$.ajax({
										url: src,
										dataType: "script",
										cache: true,
										async: false,
										error: function (xhr, status, err) {
											console.error("Fehler beim Laden von Script:", src, status, err);
										},
									});
								} else if (code.trim() !== "") {
									$.globalEval(code);
								}
							} catch (scriptError) {
								console.error("Fehler beim Ausführen eines Skripts:", scriptError);
							}
						});

						// URL in History übernehmen
						if (window.history && window.history.pushState) {
							window.history.pushState({ ajaxLoaded: true, url: actionUrl }, "", actionUrl);
						}
					} catch (innerError) {
						console.error("AJAX-Upload: Fehler beim Verarbeiten des Inhalts:", innerError);
						mainContent.html("<div style='color:red;padding:10px;'>Fehler beim Verarbeiten des Inhalts.</div>");
					}
				},
				error: function (jqXHR, textStatus, errorThrown) {
					console.error("AJAX-Upload: Fehler beim Request:", textStatus, errorThrown);
					var errorMsg = "<div style='color:red;padding:10px;'>Fehler beim Upload:<br>" +
						$("<div>").text(textStatus + ": " + errorThrown).html() + "</div>";
					mainContent.html(errorMsg);
				}
			});

		} catch (error) {
			console.error("AJAX-Upload: Allgemeiner Fehler:", error);
			$("#main_content").html("<div style='color:red;padding:10px;'>Ein unerwarteter Fehler ist aufgetreten.</div>");
		}
	});
}

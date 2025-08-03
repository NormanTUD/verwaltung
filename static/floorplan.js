// TODO: Snapping mit mehreren Snapfeldern geht noch nicht

let params;
let scale = 1;
const räume = {};

try {
	params = new URLSearchParams(window.location.search);
} catch (error) {
	console.error("Fehler beim Parsen der URL:", error);
	params = new URLSearchParams(); // Leeres Fallback
}

let building_id_str = params.get("building_id");
let etage_str = params.get("etage");

let building_id = parseInt(building_id_str, 10);
let etage = parseInt(etage_str, 10);

function getElementMouseOffset(e, el) {
	const elRect = el.getBoundingClientRect();
	const offsetX = e.clientX - elRect.left;
	const offsetY = e.clientY - elRect.top;
	return { offsetX, offsetY };
}

function findRaumContainingElementCenter(el) {
	const objRect = el.getBoundingClientRect();
	const cx = objRect.left + objRect.width / 2;
	const cy = objRect.top + objRect.height / 2;
	//console.log("Element center coordinates:", { cx, cy });

	let foundRaum = null;
	Object.values(räume).forEach(raum => {
		const rRect = raum.el.getBoundingClientRect();
		if (cx > rRect.left && cx < rRect.right && cy > rRect.top && cy < rRect.bottom) {
			foundRaum = raum;
			//console.log("Found raum containing element:", raum.el.dataset.name);
		}
	});
	if (!foundRaum) console.log("No raum found containing element");
	return foundRaum;
}

function removeFromOldRaum(el) {
	const attributes = JSON.parse(el.dataset.attributes || "{}");
	const personId = attributes.id;
	const raumId = attributes.raum_id;  // Das ist die Zahl, die das Backend braucht

	if (!personId || !raumId) {
		console.warn("Fehlende Person- oder Raum-ID beim Entfernen aus altem Raum");
		return;
	}

	// Entferne das Element aus dem lokalen Raum-Objekt, falls vorhanden
	const oldRaumName = el.dataset.raum;
	if (räume[oldRaumName]) {
		räume[oldRaumName].objects = räume[oldRaumName].objects.filter(o => o !== el);
		console.log(`Removed element from old raum: ${oldRaumName}`);
	}

	// API-Call mit korrekter Raum-ID (Zahl)
	const url = `/api/delete_person_from_raum?person_id=${personId}&raum_id=${raumId}`;
	fetch(url, { method: "GET" })
		.then(response => {
			if (!response.ok) throw new Error(`API Fehler: ${response.status}`);
			return response.json();
		})
		.then(data => {
			console.log("Person wurde aus altem Raum entfernt:", data);

			// Raum-ID im attributes auf null setzen, weil Person jetzt aus diesem Raum raus ist
			attributes.raum_id = null;
			el.dataset.attributes = JSON.stringify(attributes);
		})
		.catch(err => {
			console.error("Fehler beim Entfernen der Person aus dem alten Raum:", err);
		});
}

function addToNewRaum(el, newRaum) {
	newRaum.objects.push(el);
	log("newRaum:", newRaum);
	el.dataset.raum = newRaum.el.dataset.id;
	console.log(`Added element to new raum: ${newRaum.el.dataset.id}`, el);

	// Nur fortfahren, wenn es sich um eine Person handelt
	if (!el.classList.contains("person-circle")) {
		return;
	}

	var attributes = JSON.parse(el.dataset.attributes || "{}");

	const payload = {
		raum: newRaum.el.dataset.id,
		person: attributes,
		x: parseInt($(el).css("left")),
		y: parseInt($(el).css("top"))
	};

	if (payload.raum === undefined) {
		alert("payload.raum ist undefined!");
		return;
	}

	fetch("/api/save_person_to_raum", {
		method: "POST",
		headers: {
			"Content-Type": "application/json"
		},
		body: JSON.stringify(payload)
	})
		.then(response => {
			if (!response.ok) throw new Error("Netzwerkantwort war nicht OK");
			return response.json();
		})
		.then(data => {
			console.log("Erfolgreich gespeichert:", data);

			var currentAttributes = $(el).attr('data-attributes');
			var attributesObj = currentAttributes ? JSON.parse(currentAttributes) : {};
			attributesObj.raum_id = data.raum_id;
			$(el).attr('data-attributes', JSON.stringify(attributesObj));
		})
		.catch(error => {
			console.error("Fehler beim Speichern:", error);
		});
}

// Hilfsfunktion: Formular generieren
function generateForm(schema, formElement) {
	formElement.innerHTML = ""; // Formular leeren

	schema.forEach(field => {
		const label = document.createElement("label");
		label.textContent = field.label;
		label.style.display = "block"; // Label als Block, damit das Input darunter steht

		const input = document.createElement("input");
		input.type = "text";
		input.name = field.key;
		input.style.display = "block"; // Input als Block, damit es unter dem Label steht
		input.style.marginBottom = "10px"; // Abstand nach unten

		formElement.appendChild(label);
		formElement.appendChild(input);
	});
}

// Bestehende Personen in Select füllen
function populateExistingPersonSelect() {
	existingPersonSelect.innerHTML = "";
	personDatabase.forEach((person, index) => {
		const option = document.createElement("option");
		option.value = index;

		// Titel, falls vorhanden, sonst leerer String
		const title = person.title ? person.title + " " : "";

		option.textContent = `${title}${person.vorname} ${person.nachname}`;
		existingPersonSelect.appendChild(option);
		console.log(person);
	});
}

// Anzeigen je nach Modus (select oder new)
function updateFormMode() {
	const mode = document.querySelector('input[name="mode"]:checked').value;

	if (mode === "select") {
		dynamicForm.style.display = "none";
		document.getElementById("selectPersonArea").style.display = "block";
	} else {
		generateForm(personSchema, dynamicForm);
		dynamicForm.style.display = "block";
		document.getElementById("selectPersonArea").style.display = "none";
	}
}

function createLabel(name) {
	const label = document.createElement("div");
	label.className = "raum-label";
	//label.textContent = "Raum " + name; 
	return label;
}

function createRaum(data) {
	const raum = createRaumElement(data);
	const label = createLabel(data.name);

	raum.appendChild(label);

	return raum;
}

function createRäume() {
	räumeData.forEach(data => {
		const raum = createRaum(data);
		floorplan.appendChild(raum);

		räume[data.name] = {
			el: raum,
			objects: [], // ← Wichtig: Wird zur Laufzeit ergänzt, keine Änderung an räumeData nötig
		};
	});
}

function createRaumElement(data) {
	const raum = document.createElement("div");
	raum.className = "raum";
	raum.style.left = data.x + "px";
	raum.style.top = data.y + "px";

	if (data.width) {
		raum.style.width = data.width + "px";
	}
	if (data.height) {
		raum.style.height = data.height + "px";
	}

	raum.dataset.name = data.name;
	raum.dataset.id = data.id;
	return raum;
}

function updateZIndex(obj, raum) {
	obj.style.zIndex = 300;
}

function makeDraggable(el) {
	let dragging = false;
	let dragOffsetX = 0;
	let dragOffsetY = 0;

	function stopDragging() {
		if (!dragging) return;
		dragging = false;
		el.style.cursor = "grab";
		//console.log("Dragging stopped");

		document.removeEventListener("mousemove", onMouseMove);
		document.removeEventListener("mouseup", onMouseUp);

		const foundRaum = findRaumContainingElementCenter(el);

		if (foundRaum) {
			//console.log("Found raum on drag end:", foundRaum);

			removeFromOldRaum(el);
			addToNewRaum(el, foundRaum);

			updateZIndex(el, foundRaum);
			// snapObjectToZone(el, foundRaum); ← DAS WEG!
		} else {
			console.log("No raum found on drag end");
		}
		checkIfObjectOnPerson(el);
	}

	function checkIfObjectOnPerson(el) {
		console.log("🚧 Überprüfe, ob 'el' das Zielobjekt ist oder eine Person:");

		if (el.classList.contains("person-circle")) {
			console.warn("⚠️ Das verschobene Element ist eine Person! Es sollte kein Person-Element entfernt werden.");
			return;
		}

		const objRect = el.getBoundingClientRect();
		const objCenterX = objRect.left + objRect.width / 2;
		const objCenterY = objRect.top + objRect.height / 2;

		console.log("🔍 Objekt-Mitte:", objCenterX, objCenterY);

		const personEls = document.querySelectorAll('.person-circle');
		let found = false;

		personEls.forEach(person => {
			const personRect = person.getBoundingClientRect();

			const hit =
				objCenterX >= personRect.left &&
				objCenterX <= personRect.right &&
				objCenterY >= personRect.top &&
				objCenterY <= personRect.bottom;

			console.log(`👤 Prüfe Person ${person.id || "[kein ID]"}: Treffer?`, hit);

			if (hit) {
				found = true;

				// Objekt zum Inventar hinzufügen
				const attributes = JSON.parse(person.dataset.attributes || "{}");

				if (!attributes.inventory) {
					attributes.inventory = [];
				}

				const objectOptions = JSON.parse(el.dataset.attributes || "{}");
				attributes.inventory.push(objectOptions);
				person.dataset.attributes = JSON.stringify(attributes);

				console.log("📦 Objekt zum Inventar hinzugefügt:", objectOptions);

				// Objekt aus DOM entfernen (Objekt verschwindet vom Floorplan)
				el.remove();
				console.log("🗑️ Objekt wurde aus DOM entfernt");

				// Optional: dataset.inventory auch aktualisieren, falls du es nutzt
				try {
					let inventory = JSON.parse(person.dataset.inventory || "[]");
					inventory.push(objectOptions);
					person.dataset.inventory = JSON.stringify(inventory);
				} catch (err) {
					console.warn("⚠️ Fehler beim Parsen von inventory, setze auf leer");
				}

				// Kontextmenü updaten (falls offen)
				updateContextMenuInventory(person);

				return;
			}
		});

		if (!found) {
			console.log("❌ Objekt befindet sich auf keiner Person.");
		}
	}


	function onMouseUp(ev) {
		stopDragging();
	}

	function getMousePosRelativeToViewport(ev) {
		const floorplanRect = $("#viewport")[0].getBoundingClientRect();

		let mouseX = parseInt(ev.clientX - floorplanRect.left - dragOffsetX);
		let mouseY = parseInt(ev.clientY - floorplanRect.top - dragOffsetY);

		//console.log("Raw mouse position relative to floorplan:", { mouseX, mouseY });
		return { mouseX, mouseY };
	}

	function getMousePosRelativeToFloorplan(ev) {
		const floorplanRect = floorplan.getBoundingClientRect();

		let mouseX = parseInt(ev.clientX - floorplanRect.left - dragOffsetX);
		let mouseY = parseInt(ev.clientY - floorplanRect.top - dragOffsetY);

		//console.log("Raw mouse position relative to floorplan:", { mouseX, mouseY });
		return { mouseX, mouseY };
	}

	function scaleAndClampPosition(mouseX, mouseY) {
		let x = mouseX / scale;
		let y = mouseY / scale;

		x = Math.min(Math.max(0, x), floorplan.offsetWidth - el.offsetWidth);
		y = Math.min(Math.max(0, y), floorplan.offsetHeight - el.offsetHeight);

		//console.log("Scaled and clamped position:", { x, y });
		return { x, y };
	}

	function moveElement(x, y) {
		el.style.left = x + "px";
		el.style.top = y + "px";
		el.dataset.snapped = "false";
		//console.log(`Element moved to (${x}, ${y})`);
	}

	function onMouseMove(ev) {
		if (!dragging) return;

		const { mouseX, mouseY } = getMousePosRelativeToFloorplan(ev);

		//log(`onMouseMove: Mouse position relative to floorplan: ${mouseX}, ${mouseY}`);

		const { x, y } = scaleAndClampPosition(mouseX, mouseY);

		//log(`onMouseMove: Scaled and clamped position: ${x}, ${y}`);

		moveElement(x, y);
	}

	function onMouseMoveViewport(ev) {
		if (!dragging) return;

		const { mouseX, mouseY } = getMousePosRelativeToViewport(ev);

		//log(`onMouseMove: Mouse position relative to floorplan: ${mouseX}, ${mouseY}`);

		const { x, y } = scaleAndClampPosition(mouseX, mouseY);

		//log(`onMouseMove: Scaled and clamped position: ${x}, ${y}`);

		moveElement(x, y);
	}

	function startDragging(e) {
		e.preventDefault();

		dragging = true;
		el.style.cursor = "grabbing";
		//console.log("Dragging started");

		const offsets = getElementMouseOffset(e, el);
		dragOffsetX = offsets.offsetX;
		dragOffsetY = offsets.offsetY;

		//log(`startDragging: Element mouse offset: ${dragOffsetX}, ${dragOffsetY}`);

		//log(e.target.offsetParent);

		if (e.target.offsetParent.classList.contains("person-circle")) {
			document.addEventListener("mousemove", onMouseMove);
		} else {
			document.addEventListener("mousemove", onMouseMoveViewport)
		}
		document.addEventListener("mousemove", onMouseMoveViewport);
		document.addEventListener("mouseup", onMouseUp);
	}


	el.addEventListener("mousedown", (e) => {
		if (e.button === 2) return; // Rechtsklick -> Kontextmenü bleibt erlaubt
		removeExistingContextMenus(); // ❗ Kontextmenü schließen beim Start des Drag
		startDragging(e); // Drag starten
	});
}

async function loadPersonDatabase() {
	try {
		const response = await fetch("/api/get_person_database");
		if (!response.ok) {
			throw new Error("Fehler beim Laden der Personendaten");
		}

		const data = await response.json();
		personDatabase = data;

		//console.log("✅ Personendaten erfolgreich geladen:", personDatabase);
		return personDatabase; // optional: Rückgabe, falls du die Daten weiterverwenden willst
	} catch (error) {
		console.error("❌ Fehler beim Laden der Personendaten:", error);
		return [];
	}
}

if (!isNaN(building_id) && !isNaN(etage)) {
	const floorplan = document.getElementById("floorplan");
	let offsetX = 0;
	let offsetY = 0;
	let objId = 0;
	let selectedShape = null;
	let startPanX = 0;
	let startPanY = 0;
	let startOffsetX = 0;
	let startOffsetY = 0;

	var räumeData = [];

	function loadFloorplan() {
		if (typeof building_id !== "number" || typeof etage !== "number") {
			console.error("loadFloorplan: building_id und etage müssen Zahlen sein");
			return;
		}

		var url = "/get_floorplan?building_id=" + encodeURIComponent(building_id) + "&etage=" + encodeURIComponent(etage);

		fetch(url)
			.then(function (response) {
				if (!response.ok) {
					throw new Error("Serverantwort war nicht OK: " + response.status + " " + response.statusText);
				}
				return response.json();
			})
			.then(function (data) {
				if (!Array.isArray(data)) {
					throw new Error("Antwort ist kein Array: " + JSON.stringify(data));
				}

				räumeData = data;

				createRäume();
			})
			.catch(function (error) {
				console.error("Fehler beim Laden des Floorplans:", error);
			});
	}

	// Globale Personendatenbank
	let personDatabase = [];

	const addPersonBtn = document.getElementById("addPersonBtn");
	const personForm = document.getElementById("personForm");
	const dynamicForm = document.getElementById("dynamicPersonForm");
	const confirmPersonBtn = document.getElementById("confirmPersonBtn");
	const existingPersonSelect = document.getElementById("existingPersonSelect");

	if (addPersonBtn) {
		addPersonBtn.addEventListener("click", () => {
			personForm.style.display = "block";
			populateExistingPersonSelect();
			updateFormMode();
			applyInvertFilterToElements(theme)
			cancelBtnFunction()
		});
	}

	// Radio Buttons für Modus wechseln
	document.querySelectorAll('input[name="mode"]').forEach(radio => {
		radio.addEventListener("change", updateFormMode);
	});

	confirmPersonBtn.addEventListener("click", () => {
		try {
			const mode = getSelectedMode();

			if (mode === "select") {
				handleSelectMode();
			} else {
				handleCreateMode();
			}

			resetForm();
		} catch (error) {
			console.error("Fehler im Haupt-Event-Handler:", error);
		}
	});

	function getSelectedMode() {
		const modeInput = document.querySelector('input[name="mode"]:checked');
		if (!modeInput) {
			console.error("Kein Modus ausgewählt.");
			throw new Error("Bitte einen Modus auswählen.");
		}
		//console.log("Modus gewählt:", modeInput.value);
		return modeInput.value;
	}

	function handleSelectMode() {
		const selectedIndex = existingPersonSelect.value;
		if (selectedIndex === "") {
			alert("Bitte eine Person auswählen!");
			console.warn("Keine Person ausgewählt.");
			return;
		}

		const person = personDatabase[selectedIndex];
		if (!person) {
			console.error("Person an ausgewähltem Index nicht gefunden:", selectedIndex);
			return;
		}

		//console.log("Existierende Person ausgewählt:", person);
		createPersonCircle(person);
	}

	function resetForm() {
		personForm.style.display = "none";
		dynamicForm.innerHTML = "";
		dynamicForm.style.display = "none";
		//console.log("Formular zurückgesetzt.");
	}

	async function savePersonToDatabase(newPerson) {
		try {
			const response = await fetch('/api/add_or_update_person', {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify(newPerson)
			});

			if (!response.ok) {
				const errorData = await response.json();
				console.error('Fehler beim Speichern:', errorData.error);
				return;
			}

			const result = await response.json();
			console.log('Person erfolgreich gespeichert:', result.message);
		} catch (error) {
			console.error('Netzwerkfehler:', error);
		}
	}

	// Erstelle Person-Kreis und hänge an Floorplan an
	function createPersonCircle(attributes) {
		const circle = createCircleElement(attributes);
		addCircleToFloorplan(circle);
		makeDraggable(circle);
		setupContextMenu(circle, attributes);
		//add_or_update_person(attributes);
		applyInvertFilterToElements(theme)
	}

	function getPersonRaumDataSync() {
		var xhr = new XMLHttpRequest();
		var url = "/api/get_person_raum_data?building_id=" + encodeURIComponent(building_id) + "&etage=" + encodeURIComponent(etage);
		xhr.open("GET", url, false); // synchron
		try {
			xhr.send(null);
			if (xhr.status === 200) {
				return JSON.parse(xhr.responseText);
			} else {
				console.error("HTTP-Fehler:", xhr.status);
				return null;
			}
		} catch (e) {
			console.error("XHR-Fehler:", e);
			return null;
		}
	}

	function createCircleElement(attributes, position = null) {
		const circle = document.createElement("div");
		circle.classList.add("person-circle");
		circle.dataset.attributes = JSON.stringify(attributes);
		circle.style.position = "absolute";

		// 🔴 ❌ Button oben rechts
		const closeBtn = document.createElement("div");
		closeBtn.className = "circle-close-button";
		closeBtn.textContent = "×";
		closeBtn.title = "Entfernen";

		closeBtn.addEventListener("click", (e) => {
			e.stopPropagation();

			const attrs = JSON.parse(circle.dataset.attributes);
			const personId = attrs.id; // ggf anpassen, wenn anders benannt
			const raumId = attrs.raum_id; // nehme an etage ist raumId

			if (!personId || raumId === undefined) {
				console.error("Person ID oder Raum ID fehlt:", personId, raumId);
				return;
			}

			const url = `/api/delete_person_from_raum?person_id=${personId}&raum_id=${raumId}`;

			fetch(url, { method: 'GET' })
				.then(response => {
					if (!response.ok) {
						throw new Error(`API Fehler: ${response.status}`);
					}
					return response.json();
				})
				.then(data => {
					console.log("Person wurde aus Raum entfernt:", data);
					floorplan.removeChild(circle);
				})
				.catch(err => {
					console.error("Fehler beim Entfernen der Person aus dem Raum:", err);
				});
		});

		circle.appendChild(closeBtn);

		// 🖼️ Bild oder Name
		if (attributes.image_url) {
			const img = document.createElement("img");
			img.src = attributes.image_url;
			img.alt = `${attributes.vorname} ${attributes.nachname}`;
			img.style.zIndex = "1"; // liegt unter dem Button
			circle.appendChild(img);
		} else {
			const nameSpan = document.createElement("span");
			nameSpan.className = "no-image";
			nameSpan.style.color = "black";
			nameSpan.style.zIndex = "1";
			nameSpan.innerHTML = `${attributes.vorname}<br>${attributes.nachname}`;
			circle.appendChild(nameSpan);
		}

		setCirclePosition(circle, position);
		return circle;
	}

	function getViewportCenterPosition() {
		return {
			x: window.pageXOffset + window.innerWidth / 2,
			y: window.pageYOffset + window.innerHeight / 2
		};
	}

	function load_persons_from_db() {
		const personData = getPersonRaumDataSync(building_id, etage);

		if (personData) {
			createPersonsFromApiData(personData);
		} else {
			console.error("Keine Personendaten geladen");
		}
	}

	function createPersonsFromApiData(personDataArray) {
		if (!Array.isArray(personDataArray)) {
			console.error("API-Daten sind kein Array");
			return;
		}

		for (const personEntry of personDataArray) {
			const personAttributes = personEntry.person;

			if (!personAttributes) {
				console.warn("Person ohne Attribute übersprungen");
				continue;
			}

			if (Array.isArray(personEntry.räume) && personEntry.räume.length > 0) {
				for (const raumEntry of personEntry.räume) {

					log("raumEntry:", raumEntry);
					const layout = raumEntry.layout || null;
					const position = extractPositionFromLayout(layout);

					personAttributes["raum_id"] = raumEntry.raum.id || null;

					const circle = createCircleElement(personAttributes, position);

					// Kreis an Floorplan anhängen
					floorplan.appendChild(circle);

					makeDraggable(circle);
				}
			} else {
				// Keine Räume, Kreis zentriert erzeugen
				const circle = createCircleElement(personAttributes, null);
				floorplan.appendChild(circle);
			}
		}

		applyInvertFilterToElements(theme)
	}

	function extractPositionFromLayout(layout) {
		if (!layout || typeof layout.x !== "number" || typeof layout.y !== "number") {
			return null;
		}
		return { x: layout.x, y: layout.y };
	}

	function setCirclePosition(circle, position = null) {
		const center = getViewportCenterPosition();

		const width = circle.offsetWidth || 50;
		const height = circle.offsetHeight || 50;

		circle.style.position = "absolute";

		if (position && typeof position.x === "number" && typeof position.y === "number") {
			// Position aus layout benutzen
			// Optional: ggf. Skalierung anpassen, wenn layout-Koordinaten nicht direkt px sind
			circle.style.left = position.x + "px";
			circle.style.top = position.y + "px";
		} else {
			// Standard: zentriert
			circle.style.left = `${center.x - width / 2}px`;
			circle.style.top = `${center.y - height / 2}px`;
		}
	}

	function my_escape(str) {
		if (typeof str !== 'string') {
			str = String(str ?? ''); // Konvertiert null/undefined zu leerem String
		}
		return str.replace(/[&<>"']/g, function (char) {
			const escapeChars = {
				'&': '&amp;',
				'<': '&lt;',
				'>': '&gt;',
				'"': '&quot;',
				"'": '&#39;',
			};
			return escapeChars[char];
		});
}

function addCircleToFloorplan(circle) {
	try {
		floorplan.appendChild(circle);
	} catch (error) {
		console.error("Fehler beim Hinzufügen des Kreises zum Floorplan:", error);
	}
}

function setupContextMenu(circle, attributes) {
	try {
		console.log("setupContextMenu wird aufgerufen für:", circle, "mit attributes:", attributes);

		circle.addEventListener("contextmenu", (e) => {
			console.log("Rechtsklick erkannt auf:", circle);
			e.preventDefault();
			toggleContextMenu(circle, attributes);
		});

		console.log("EventListener für Kontextmenü erfolgreich hinzugefügt.");
	} catch (error) {
		console.error("Fehler beim Einrichten des Kontextmenüs:", error);
	}
}

function toggleContextMenu(circle, attributes) {
	try {
		console.log("toggleContextMenu aufgerufen mit circle:", circle);
		console.log("toggleContextMenu attributes:", attributes);

		removeExistingContextMenus();

		// Wichtig: circle mitgeben
		const menu = buildContextMenu(attributes, circle);
		console.log("Kontextmenü gebaut:", menu);

		positionContextMenuAbsolute(circle, menu);
		floorplan.appendChild(menu);
		requestAnimationFrame(() => {
			menu.style.opacity = "1";
		});

		updateContextMenuInventory(circle);
		applyInvertFilterToElements(theme);

		console.log("Kontextmenü angezeigt:", attributes);
	} catch (error) {
		console.error("Fehler beim Umschalten des Kontextmenüs:", error);
	}
}

function removeExistingContextMenus() {
	const foundMenus = document.querySelectorAll(".context-menu");
	foundMenus.forEach(menu => menu.remove());
}

function positionContextMenuAbsolute(circle, menu) {
	const circleRect = circle.getBoundingClientRect();
	const floorRect = floorplan.getBoundingClientRect();

	// Berechne absolute Position relativ zum floorplan
	const top = circleRect.bottom - floorRect.top + 4; // 4px Abstand
	const left = circleRect.left - floorRect.left + (circleRect.width / 2);

	menu.style.position = "absolute";
	menu.style.top = `${top}px`;
	menu.style.left = `${left}px`;
	menu.style.transform = "translateX(-50%)";
}

function buildContextMenu(attributes, personEl) {
	const menu = document.createElement("div");
	menu.classList.add("context-menu");

	// Styles anwenden
	const styles = getContextMenuStyles();
	Object.assign(menu.style, styles);

	// Grundstruktur mit allen Attributen
	menu.innerHTML = `
	    <div><strong>Vorname:</strong> ${my_escape(attributes.vorname || "")}</div>
	    <div><strong>Nachname:</strong> ${my_escape(attributes.nachname || "")}</div>
	    <div><strong>Titel:</strong> ${my_escape(attributes.title || "")}</div>
	    <div><strong>Kommentar:</strong> ${my_escape(attributes.comment || "")}</div>

	    <hr>
	    <div><strong>Inventar:</strong></div>
	    <ul class="question-list" style="list-style:none; padding-left:0; margin:0;"></ul>
	  `;

	const inventory = attributes.inventory || [];
	const ul = menu.querySelector("ul.question-list");

	if (inventory.length === 0) {
		const li = document.createElement("li");
		li.textContent = "Inventar ist leer";
		ul.appendChild(li);
	} else {
		inventory.forEach((item, index) => {
			const li = document.createElement("li");
			li.style.display = "flex";
			li.style.justifyContent = "space-between";
			li.style.alignItems = "center";
			li.style.padding = "2px 4px";
			li.style.borderBottom = "1px solid #eee";

			// Item-Beschreibung als Text (z.B. alle Werte als String)
			const text = document.createElement("span");
			text.textContent = Object.values(item).join(", ");

			// Lösch-Kreuz-Button
			const deleteBtn = document.createElement("button");
			deleteBtn.textContent = "✖";
			deleteBtn.title = "Objekt entfernen";
			deleteBtn.style.cursor = "pointer";
			deleteBtn.style.border = "none";
			deleteBtn.style.background = "transparent";
			deleteBtn.style.color = "#900";
			deleteBtn.style.fontWeight = "bold";
			deleteBtn.style.fontSize = "14px";
			deleteBtn.style.padding = "0 4px";

			deleteBtn.addEventListener("click", (e) => {
				e.stopPropagation(); // Verhindert das Schließen des Menüs o.Ä.
				if (!personEl) {
					console.error("Kein personEl vorhanden zum Entfernen");
					return;
				}
				console.log(`🔴 Lösche Item Index ${index} aus Inventar von Person`, personEl);

				removeObjectFromInventory(personEl, index);

				// Kontextmenü neu bauen, da sich Inventar geändert hat
				removeExistingContextMenus();
				toggleContextMenu(personEl, JSON.parse(personEl.dataset.attributes));
			});

			li.appendChild(text);
			li.appendChild(deleteBtn);
			ul.appendChild(li);
		});
	}

	return menu;
}

function getContextMenuStyles() {
	return {
		position: "absolute",
		top: "100%",
		left: "50%",
		transform: "translateX(-50%)",
		backgroundColor: "#fff",
		border: "1px solid #ccc",
		boxShadow: "0 2px 5px rgba(0,0,0,0.2)",
		padding: "8px",
		fontSize: "12px",
		zIndex: 11,  // <- muss größer sein als der zIndex anderer Elemente IM Kreis
		marginTop: "4px",
		minWidth: "150px",
		textAlign: "left"
	};
}

function positionContextMenu(circle, menu) {
	try {
		// bereits top: 100% + marginTop in CSS
		// relative zu circle platzieren
		circle.style.position = "relative";
	} catch (error) {
		console.error("Fehler beim Positionieren des Kontextmenüs:", error);
	}
}

const addBtn = document.getElementById("addBtn");
const objectForm = document.getElementById("objectForm");
const cancelObjectBtn = document.getElementById("cancelObjectBtn");

if (addBtn) {
	addBtn.addEventListener("click", () => {
		objectForm.style.display = "block";
		cancelpersonBtnFunction()
	});
}

function cancelBtnFunction() {
	objectForm.style.display = "none";
	// Optional: Felder leeren
	document.getElementById("option1").value = "";
	document.getElementById("option2").value = "";
	document.getElementById("option3").value = "";
	document.getElementById("option4").value = "";
}

cancelObjectBtn.addEventListener("click", cancelBtnFunction);

function getInputValue(id) {
	const input = document.getElementById(id);
	//console.log(`getInputValue: id=${id}, element found? ${input !== null}`);
	if (!input) return null;
	return input.value.trim();
}

function getAllOptions() {
	const options = {
		option1: getInputValue("option1"),
		option2: getInputValue("option2"),
		option3: getInputValue("option3"),
		option4: getInputValue("option4")
	};
	//console.log("getAllOptions:", options);
	return options;
}

function createOptionsDiv(options) {
	console.log("createOptionsDiv mit Optionen:", options);
	const div = document.createElement("div");
	div.className = "optionContainer";
	div.style.position = "absolute";
	div.style.cursor = "grab";
	div.style.visibility = "hidden";

	div.dataset.attributes = JSON.stringify(options);
	div.dataset.raum = "";

	div.innerHTML = `
	    <p><strong>Option 1:</strong> ${options.option1}</p>
	    <p><strong>Option 2:</strong> ${options.option2}</p>
	    <p><strong>Option 3:</strong> ${options.option3}</p>
	    <p><strong>Option 4:</strong> ${options.option4}</p>
	  `;

	// Temporär anhängen, um die Größe zu messen
	document.body.appendChild(div);
	const { offsetWidth: width, offsetHeight: height } = div;
	document.body.removeChild(div);

	// Position im Viewport (Mitte vom Fenster)
	const centerXInViewport = window.innerWidth / 2;
	const centerYInViewport = window.innerHeight / 2;

	// Umrechnen in Koordinaten relativ zum floorplan
	const floorplanRect = floorplan.getBoundingClientRect();
	const x = centerXInViewport - floorplanRect.left - width / 2;
	const y = centerYInViewport - floorplanRect.top - height / 2;

	div.style.left = `${x}px`;
	div.style.top = `${y}px`;
	div.style.visibility = "visible";

	floorplan.appendChild(div);
	makeDraggable(div);

	return div;
}

function appendToContainer(div, containerId = "generatedObjectsContainer") {
	const container = document.getElementById(containerId);
	console.log(`appendToContainer: Container mit ID '${containerId}' gefunden? ${container !== null}`);
	if (!container) {
		console.error(`FEHLER: Container mit ID '${containerId}' nicht gefunden!`);
		return;
	}
	container.appendChild(div);
	console.log("appendToContainer: Div hinzugefügt");
	applyInvertFilterToElements(theme) 
}

function clearFormFields() {
	["option1", "option2", "option3", "option4"].forEach(id => {
		const input = document.getElementById(id);
		if (input) {
			input.value = "";
			console.log(`clearFormFields: Feld '${id}' geleert`);
		} else {
			console.warn(`clearFormFields: Feld '${id}' nicht gefunden`);
		}
	});
}

function hideForm() {
	const form = document.getElementById("objectForm");
	if (form) {
		form.style.display = "none";
		console.log("hideForm: Formular ausgeblendet");
	} else {
		console.warn("hideForm: Formular mit ID 'objectForm' nicht gefunden");
	}
}

function showForm() {
	const form = document.getElementById("objectForm");
	if (form) {
		form.style.display = "block";
		console.log("showForm: Formular angezeigt");
	} else {
		console.warn("showForm: Formular mit ID 'objectForm' nicht gefunden");
	}
}

function handleSave() {
	console.log("handleSave: Start");
	const options = getAllOptions();
	const newDiv = createOptionsDiv(options);
	appendToContainer(newDiv);
	clearFormFields();
	hideForm();
	console.log("handleSave: Fertig");
	applyInvertFilterToElements(theme)
}

function setupEventListeners() {
	const saveBtn = document.getElementById("saveOptionsBtn");
	const cancelBtn = document.getElementById("cancelObjectBtn");

	if (saveBtn) {
		saveBtn.addEventListener("click", handleSave);
		//console.log("setupEventListeners: Listener für Speichern gesetzt");
	} else {
		console.error("setupEventListeners: Button 'saveOptionsBtn' nicht gefunden");
	}

	if (cancelBtn) {
		cancelBtn.addEventListener("click", hideForm);
		//console.log("setupEventListeners: Listener für Abbrechen gesetzt");
	} else {
		console.error("setupEventListeners: Button 'cancelObjectBtn' nicht gefunden");
	}
}

window.addEventListener("DOMContentLoaded", () => {
	//console.log("DOM vollständig geladen");
	setupEventListeners();

	// Automatisch div mit aktuellen Eingaben erstellen, falls vorhanden
	const options = getAllOptions();

	// Prüfen, ob mindestens ein Eingabefeld ausgefüllt ist
	if (Object.values(options).some(value => value)) {
		const newDiv = createOptionsDiv(options);
		appendToContainer(newDiv);
	}
});

function updateContextMenuInventory(personEl) {
	const menu = document.querySelector(".context-menu");
	if (!menu) {
		console.log("ℹ️ Kein Kontextmenü offen, Inventar wird nicht angezeigt.");
		return;
	}

	const ul = menu.querySelector(".question-list");
	if (!ul) {
		console.warn("❌ Keine <ul class='question-list'> im Menü gefunden.");
		return;
	}

	let attributes = {};
	try {
		attributes = JSON.parse(personEl.dataset.attributes || "{}");
	} catch (err) {
		console.error("❌ Fehler beim Parsen der Personen-Attribute:", err);
		return;
	}

	const inventory = attributes.inventory || [];
	ul.innerHTML = "";

	if (inventory.length === 0) {
		const li = document.createElement("li");
		li.textContent = "Inventar ist leer";
		ul.appendChild(li);
	} else {
		inventory.forEach((item, index) => {
			const li = document.createElement("li");
			li.style.display = "flex";
			li.style.justifyContent = "space-between";
			li.style.alignItems = "center";
			li.style.padding = "2px 4px";
			li.style.borderBottom = "1px solid #eee";

			const text = document.createElement("span");
			text.textContent = Object.values(item).join(", ");

			const deleteBtn = document.createElement("button");
			deleteBtn.textContent = "✖";
			deleteBtn.title = "Objekt entfernen";
			deleteBtn.style.cursor = "pointer";
			deleteBtn.style.border = "none";
			deleteBtn.style.background = "transparent";
			deleteBtn.style.color = "#900";
			deleteBtn.style.fontWeight = "bold";
			deleteBtn.style.fontSize = "14px";
			deleteBtn.style.padding = "0 4px";

			deleteBtn.addEventListener("click", (e) => {
				e.stopPropagation();
				removeObjectFromInventory(personEl, index);
				updateContextMenuInventory(personEl);
			});

			li.appendChild(text);
			li.appendChild(deleteBtn);
			ul.appendChild(li);
		});
	}
}

function removeObjectFromInventory(personEl, itemIndex) {
	// Person-Attribute parsen
	let attributes = {};
	try {
		attributes = JSON.parse(personEl.dataset.attributes || "{}");
	} catch {
		console.error("Fehler beim Parsen der Personen-Attribute");
		return;
	}

	if (!attributes.inventory || !Array.isArray(attributes.inventory)) {
		console.warn("Kein Inventar gefunden");
		return;
	}

	// Objekt aus dem Inventar entfernen
	const removedItem = attributes.inventory.splice(itemIndex, 1)[0];

	// Attribute aktualisieren
	personEl.dataset.attributes = JSON.stringify(attributes);

	// Falls personEl.dataset.inventory separat gepflegt wird:
	try {
		let inv = JSON.parse(personEl.dataset.inventory || "[]");
		inv.splice(itemIndex, 1);
		personEl.dataset.inventory = JSON.stringify(inv);
	} catch {
		console.warn("Fehler beim Parsen von dataset.inventory");
	}

	// Neues Objekt-Element erzeugen (wie beim normalen Erstellen)
	const newObjEl = createOptionsDiv(removedItem);

	// An richtigen Container anhängen
	appendToContainer(newObjEl);

	console.log("✅ Objekt wurde aus Inventar entfernt und neu erstellt auf dem Floorplan:", removedItem);

	// Kontextmenü aktualisieren
	updateContextMenuInventory(personEl);
}

// Initial
loadFloorplan(building_id, etage);

const cancelPersonBtn = document.getElementById("cancelPersonBtn");

cancelPersonBtn.addEventListener("click", cancelpersonBtnFunction);

function cancelpersonBtnFunction() {
	personForm.style.display = "none";
	dynamicForm.innerHTML = "";
	dynamicForm.style.display = "none";
}

document.addEventListener("DOMContentLoaded", function () {
	loadPersonDatabase();
	load_persons_from_db();
});

document.addEventListener('keydown', function (event) {
	if (event.key === 'Escape') {
		cancelpersonBtnFunction();
		cancelBtnFunction();
	}
});

window.addEventListener("DOMContentLoaded", () => {
	const people = document.querySelectorAll(".person-circle");

	people.forEach(person => {
		person.addEventListener("contextmenu", (event) => {
			event.preventDefault(); // verhindert das native Kontextmenü
			const attributes = JSON.parse(person.getAttribute("data-attributes"));
			toggleContextMenu(person, attributes);
		});
	});
});

}

// TODO: Snapping mit mehreren Snapfeldern geht noch nicht

let params;

try {
	params = new URLSearchParams(window.location.search);
} catch (error) {
	console.error("Fehler beim Parsen der URL:", error);
	params = new URLSearchParams(); // Leeres Fallback
}

let building_id_str = params.get("building_id");
let floor_str = params.get("floor");

let building_id = parseInt(building_id_str, 10);
let floor = parseInt(floor_str, 10);

if (isNaN(building_id)) {
	console.error("Kein gültiger 'building_id' Parameter gefunden:", building_id_str);
	building_id = 0; // Standardwert oder Fehlerbehandlung
}

if (isNaN(floor)) {
	console.error("Kein gültiger 'floor' Parameter gefunden:", floor_str);
	floor = 0; // Standardwert oder Fehlerbehandlung
}

const floorplan = document.getElementById("floorplan");
let scale = 1;
let offsetX = 0;
let offsetY = 0;
let objId = 0;
let selectedShape = null;
let startPanX = 0;
let startPanY = 0;
let startOffsetX = 0;
let startOffsetY = 0;

var roomsData = [];

function loadFloorplan(buildingId, floor) {
	if (typeof buildingId !== "number" || typeof floor !== "number") {
		console.error("loadFloorplan: buildingId und floor müssen Zahlen sein");
		return;
	}

	var url = "/get_floorplan?building_id=" + encodeURIComponent(buildingId) + "&floor=" + encodeURIComponent(floor);

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

			roomsData = data;

			createRooms();
		})
		.catch(function (error) {
			console.error("Fehler beim Laden des Floorplans:", error);
		});
}

const rooms = {};

// Räume + Snapzones erzeugen


function createLabel(name) {
	const label = document.createElement("div");
	label.className = "room-label";
	//label.textContent = "Raum " + name; 
	return label;
}

function createCounter() {
	const counter = document.createElement("div");
	counter.className = "room-counter";
	//counter.textContent = "0 Objekt(e)";
	counter.dataset.count = "0";
	return counter;
}

function createRoom(data) {
	const room = createRoomElement(data);
	const label = createLabel(data.name);
	const counter = createCounter();

	room.appendChild(label);
	room.appendChild(counter);

	return { room, counter, };
}

function createRooms() {
	roomsData.forEach(data => {
		const { room, counter } = createRoom(data);
		floorplan.appendChild(room);

		rooms[data.name] = {
			el: room,
			counterEl: counter,
			objects: [], // ← Wichtig: Wird zur Laufzeit ergänzt, keine Änderung an roomsData nötig
		};
	});
}


function createRoomElement(data) {
	const room = document.createElement("div");
	room.className = "room";
	room.style.left = data.x + "px";
	room.style.top = data.y + "px";

	if (data.width) {
		room.style.width = data.width + "px";
	}
	if (data.height) {
		room.style.height = data.height + "px";
	}

	room.dataset.name = data.name;
	return room;
}






function checkObjectRoomAssignment(el) {
	if (!el.dataset.room) {
		console.error("Fehler: Objekt hat kein zugewiesenes room-Dataset.");
		return false;
	}
	if (!rooms[el.dataset.room]) {
		console.error(`Fehler: Raum '${el.dataset.room}' existiert nicht in rooms.`);
		return false;
	}
	console.log(`Objekt ist Raum '${el.dataset.room}' zugewiesen.`);
	return true;
}

function checkDragEventListeners(el) {
	// Da wir keine einfache API haben, um das direkt zu prüfen,
	// machen wir einen kleinen Test: simulieren wir einen mousedown-Event
	// und checken, ob startDragging ausgeführt wird.  
	// (Alternative: Eventlistener speichern und prüfen, oder ein Flag)

	console.warn("Prüfung der Eventlistener kann nur indirekt erfolgen.");
	// Tipp: Bei Problemen das Drag-Verhalten beobachten.
}

function checkElementStyles(el) {
	const style = window.getComputedStyle(el);
	if (style.position !== "absolute") {
		console.error(`Fehler: Objekt-Position ist '${style.position}', sollte 'absolute' sein.`);
	} else {
		console.log("Objekt hat korrekte CSS-Position: absolute.");
	}
	if (style.pointerEvents === "none") {
		console.error("Fehler: pointer-events ist 'none', Objekt kann keine Mausereignisse erhalten.");
	}
	if (style.display === "none") {
		console.error("Fehler: Objekt hat display:none, ist also nicht sichtbar.");
	}
}

function checkParentInDOM(el) {
	if (!el.parentElement) {
		console.error("Fehler: Objekt hat kein Parent-Element im DOM.");
		return false;
	}
	if (!floorplan.contains(el)) {
		console.error("Fehler: Objekt ist nicht (mehr) im floorplan enthalten.");
		return false;
	}
	console.log("Objekt ist korrekt im floorplan enthalten.");
	return true;
}





function updateCounter(room) {
	const count = room.objects.length;
	room.counterEl.textContent = `${count} Objekt(e)`;
	room.counterEl.dataset.count = count;
}






function updateZIndex(obj, room) {
	obj.style.zIndex = 300;
}

function makeDraggable(el) {
	let dragging = false;
	let dragOffsetX = 0;
	let dragOffsetY = 0;

	function getElementMouseOffset(e, el) {
		const elRect = el.getBoundingClientRect();
		const offsetX = e.clientX - elRect.left;
		const offsetY = e.clientY - elRect.top;
		return { offsetX, offsetY };
	}


	function startDragging(e) {
		e.preventDefault();

		dragging = true;
		el.style.cursor = "grabbing";
		console.log("Dragging started");

		const offsets = getElementMouseOffset(e, el);
		dragOffsetX = offsets.offsetX;
		dragOffsetY = offsets.offsetY;

		log(`startDragging: Element mouse offset: ${dragOffsetX}, ${dragOffsetY}`);

		log(e.target.offsetParent);

		if(e.target.offsetParent.classList.contains("person-circle")) {
			document.addEventListener("mousemove", onMouseMove);
		} else {
			document.addEventListener("mousemove", onMouseMoveViewport)
		}
		document.addEventListener("mousemove", onMouseMoveViewport);
		document.addEventListener("mouseup", onMouseUp);
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

	function onMouseMoveViewport(ev) {
		if (!dragging) return;

		const { mouseX, mouseY } = getMousePosRelativeToViewport(ev);

		//log(`onMouseMove: Mouse position relative to floorplan: ${mouseX}, ${mouseY}`);

		const { x, y } = scaleAndClampPosition(mouseX, mouseY);

		//log(`onMouseMove: Scaled and clamped position: ${x}, ${y}`);

		moveElement(x, y);
	}

	function onMouseMove(ev) {
		if (!dragging) return;

		const { mouseX, mouseY } = getMousePosRelativeToFloorplan(ev);

		log(`onMouseMove: Mouse position relative to floorplan: ${mouseX}, ${mouseY}`);

		const { x, y } = scaleAndClampPosition(mouseX, mouseY);

		log(`onMouseMove: Scaled and clamped position: ${x}, ${y}`);

		moveElement(x, y);
	}

	function findRoomContainingElementCenter(el) {
		const objRect = el.getBoundingClientRect();
		const cx = objRect.left + objRect.width / 2;
		const cy = objRect.top + objRect.height / 2;
		//console.log("Element center coordinates:", { cx, cy });

		let foundRoom = null;
		Object.values(rooms).forEach(room => {
			const rRect = room.el.getBoundingClientRect();
			if (cx > rRect.left && cx < rRect.right && cy > rRect.top && cy < rRect.bottom) {
				foundRoom = room;
				console.log("Found room containing element:", room.el.dataset.name);
			}
		});
		if (!foundRoom) console.log("No room found containing element");
		return foundRoom;
	}

	function removeFromOldRoom(el) {
		const oldRoomName = el.dataset.room;
		if (rooms[oldRoomName]) {
			const oldRoom = rooms[oldRoomName];
			oldRoom.objects = oldRoom.objects.filter(o => o !== el);
			updateCounter(oldRoom);
			console.log(`Removed element from old room: ${oldRoomName}`);
		}
	}

	function addToNewRoom(el, newRoom) {
		newRoom.objects.push(el);
		el.dataset.room = newRoom.el.dataset.name;
		updateCounter(newRoom);
		console.log(`Added element to new room: ${newRoom.el.dataset.name}`, el);

		var attributes = JSON.parse(el.dataset.attributes || "{}");

		// Daten, die gesendet werden sollen (z.B. attributes plus Raum)
		const payload = {
			room: newRoom.el.dataset.name,
			person: attributes,
			x: parseInt($(el).css("left")),
			y: parseInt($(el).css("top"))
		};

		fetch("/api/save_person_to_room", {
			method: "POST",
			headers: {
				"Content-Type": "application/json"
			},
			body: JSON.stringify(payload)  // Payload als JSON-String schicken
		})
			.then(response => {
				if (!response.ok) throw new Error("Netzwerkantwort war nicht OK");
				return response.json(); // falls JSON als Antwort erwartet wird
			})
			.then(data => {
				console.log("Erfolgreich gespeichert:", data);
			})
			.catch(error => {
				console.error("Fehler beim Speichern:", error);
			});
	}

	function stopDragging() {
		if (!dragging) return;
		dragging = false;
		el.style.cursor = "grab";
		console.log("Dragging stopped");

		document.removeEventListener("mousemove", onMouseMove);
		document.removeEventListener("mouseup", onMouseUp);

		const foundRoom = findRoomContainingElementCenter(el);

		if (foundRoom) {
			console.log("Found room on drag end:", foundRoom);

			if (el.dataset.room !== foundRoom.el.dataset.name) {
				removeFromOldRoom(el);
				addToNewRoom(el, foundRoom);
			}

			updateZIndex(el, foundRoom);
			// snapObjectToZone(el, foundRoom); ← DAS WEG!
		} else {
			console.log("No room found on drag end");
			if (rooms[el.dataset.room]) {
				// snapObjectToZone(el, rooms[el.dataset.room]); ← AUCH WEG!
			}
		}
		checkIfObjectOnPerson(el);

	}




	function onMouseUp(ev) {
		stopDragging();
	}

	el.addEventListener("mousedown", (e) => {
		if (e.button === 2) return; // Rechtsklick -> Kontextmenü bleibt erlaubt
		removeExistingContextMenus(); // ❗ Kontextmenü schließen beim Start des Drag
		startDragging(e); // Drag starten
	});
}
// Globale Personendatenbank
let personDatabase = [];

async function loadPersonDatabase() {
	try {
		const response = await fetch("/api/get_person_database");
		if (!response.ok) {
			throw new Error("Fehler beim Laden der Personendaten");
		}

		const data = await response.json();
		personDatabase = data;

		console.log("✅ Personendaten erfolgreich geladen:", personDatabase);
		return personDatabase; // optional: Rückgabe, falls du die Daten weiterverwenden willst
	} catch (error) {
		console.error("❌ Fehler beim Laden der Personendaten:", error);
		return [];
	}
}


const addPersonBtn = document.getElementById("addPersonBtn");
const personForm = document.getElementById("personForm");
const dynamicForm = document.getElementById("dynamicPersonForm");
const confirmPersonBtn = document.getElementById("confirmPersonBtn");
const existingPersonSelect = document.getElementById("existingPersonSelect");

const personSchema = [
	{ label: "Vorname", key: "first_name", type: "string" },
	{ label: "Nachname", key: "last_name", type: "string" },
	{ label: "Titel", key: "title", type: "string" },
	{ label: "Kommentar", key: "comment", type: "string" },
	{ label: "Bild-URL", key: "image_url", type: "string" }
];

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
		option.textContent = `${person.first_name} ${person.last_name} (${person.rolle})`;
		existingPersonSelect.appendChild(option);
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

if(addPersonBtn) {
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
	console.log("Modus gewählt:", modeInput.value);
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

	console.log("Existierende Person ausgewählt:", person);
	createPersonCircle(person);
}

function handleCreateMode() {
	const formData = new FormData(dynamicForm);
	const newPerson = {};
	for (const field of personSchema) {
		let value = formData.get(field.key);
		if (!value) {
			alert(`Bitte das Feld "${field.label}" ausfüllen.`);
			return;
		}
		newPerson[field.key] = value;
	}
	savePersonToDatabase(newPerson);
	createPersonCircle(newPerson);
}

async function savePersonToDatabase(newPerson) {
	try {
		const response = await fetch('/api/add_person', {
			method: 'POST',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify(newPerson)
		});
		const result = await response.json();
		if (!response.ok) {
			console.error('Fehler beim Speichern:', result.error);
			return;
		}
		console.log('Person erfolgreich gespeichert:', result);
	} catch (error) {
		console.error('Netzwerkfehler:', error);
	}
}

function resetForm() {
	personForm.style.display = "none";
	dynamicForm.innerHTML = "";
	dynamicForm.style.display = "none";
	console.log("Formular zurückgesetzt.");
}



// Erstelle Person-Kreis und hänge an Floorplan an
function createPersonCircle(attributes) {
	const circle = createCircleElement(attributes);
	addCircleToFloorplan(circle);
	makeDraggable(circle);
	setupContextMenu(circle, attributes);

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
	applyInvertFilterToElements(theme)
}

function getPersonRoomDataSync(buildingId, floor) {
	var xhr = new XMLHttpRequest();
	var url = "/api/get_person_room_data?building_id=" + encodeURIComponent(buildingId) + "&floor=" + encodeURIComponent(floor);
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

	// Bild nur anzeigen
	circle.innerHTML = `<img src="${attributes.image_url}" alt="Personenbild" />`;

	setCirclePosition(circle, position);

	return circle;
}

function getScrollPosition() {
	return {
		x: window.scrollX || window.pageXOffset,
		y: window.scrollY || window.pageYOffset,
	};
}

function getViewportSize() {
	return {
		width: window.innerWidth,
		height: window.innerHeight,
	};
}

function getViewportCenterPosition() {
	return {
		x: window.pageXOffset + window.innerWidth / 2,
		y: window.pageYOffset + window.innerHeight / 2
	};
}

function load_persons_from_db () {
	const personData = getPersonRoomDataSync(building_id, floor);

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

		// Für jeden Raum der Person einen Kreis an der Layout-Position erzeugen
		if (Array.isArray(personEntry.rooms) && personEntry.rooms.length > 0) {
			for (const roomEntry of personEntry.rooms) {
				const layout = roomEntry.layout || null;
				const position = extractPositionFromLayout(layout);

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

function getCircleStyles() {
	return {
		width: "80px",
		height: "80px",
		borderRadius: "50%",
		border: "2px solid #333",
		display: "flex",
		flexDirection: "column",
		justifyContent: "center",
		alignItems: "center",
		margin: "0",
		backgroundColor: "#f0f0f0",
		boxShadow: "0 0 5px rgba(0,0,0,0.3)",
		fontFamily: "Arial, sans-serif",
		textAlign: "center",
		padding: "10px",
		position: "absolute",
		cursor: "grab",
		zIndex: 10
	};
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


function setCircleContent(circle, attributes) {
	circle.innerHTML = `
    <img src="${attributes.image_url || 'https://scads.ai/wp-content/uploads/Bicanski_Andrej-_500x500-400x400.jpg'}" style="max-width: 64px; max-height: 64px; border-radius: 50%;" />
    <strong>${my_escape(attributes.first_name)} ${my_escape(attributes.last_name)}</strong><br>
    <span>${my_escape(attributes.title)}</span><br>
    <span>${my_escape(attributes.comment)}</span>
  `;
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
		circle.addEventListener("contextmenu", (e) => {
			e.preventDefault();
			toggleContextMenu(circle, attributes);
		});
	} catch (error) {
		console.error("Fehler beim Einrichten des Kontextmenüs:", error);
	}
}

function toggleContextMenu(circle, attributes) {
	try {
		removeExistingContextMenus();

		// Wichtig: circle mitgeben
		const menu = buildContextMenu(attributes, circle);
		positionContextMenuAbsolute(circle, menu);
		floorplan.appendChild(menu);

		updateContextMenuInventory(circle);

		applyInvertFilterToElements(theme)

		console.log("Kontextmenü angezeigt:", attributes);
	} catch (error) {
		console.error("Fehler beim Umschalten des Kontextmenüs:", error);
	}
}



function removeExistingContextMenus() {
	const menus = document.querySelectorAll(".context-menu");
	menus.forEach(menu => menu.remove());
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
    <div><strong>Vorname:</strong> ${my_escape(attributes.first_name || "")}</div>
    <div><strong>Nachname:</strong> ${my_escape(attributes.last_name || "")}</div>
    <div><strong>Titel:</strong> ${my_escape(attributes.title || "")}</div>
    <div><strong>Kommentar:</strong> ${my_escape(attributes.comment || "")}</div>
    <div><strong>Bild-URL:</strong> <a href="${my_escape(attributes.image_url || "#")}" target="_blank">${my_escape(attributes.image_url || "")}</a></div>
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

if(addBtn) {
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
	console.log(`getInputValue: id=${id}, element found? ${input !== null}`);
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
	console.log("getAllOptions:", options);
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
	div.dataset.room = "";

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
		console.log("setupEventListeners: Listener für Speichern gesetzt");
	} else {
		console.error("setupEventListeners: Button 'saveOptionsBtn' nicht gefunden");
	}

	if (cancelBtn) {
		cancelBtn.addEventListener("click", hideForm);
		console.log("setupEventListeners: Listener für Abbrechen gesetzt");
	} else {
		console.error("setupEventListeners: Button 'cancelObjectBtn' nicht gefunden");
	}
}

window.addEventListener("DOMContentLoaded", () => {
	console.log("DOM vollständig geladen");
	setupEventListeners();

	// Automatisch div mit aktuellen Eingaben erstellen, falls vorhanden
	const options = getAllOptions();

	// Prüfen, ob mindestens ein Eingabefeld ausgefüllt ist
	if (Object.values(options).some(value => value)) {
		const newDiv = createOptionsDiv(options);
		appendToContainer(newDiv);
	}
});





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
loadFloorplan(building_id, floor);

const cancelPersonBtn = document.getElementById("cancelPersonBtn");

cancelPersonBtn.addEventListener("click", cancelpersonBtnFunction);


function cancelpersonBtnFunction() {
	personForm.style.display = "none";
	dynamicForm.innerHTML = "";
	dynamicForm.style.display = "none";
}

document.addEventListener("DOMContentLoaded", function() {
	loadPersonDatabase();
	load_persons_from_db();
});

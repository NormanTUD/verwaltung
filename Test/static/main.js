// TODO: Snapping mit mehreren Snapfeldern geht noch nicht


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

const roomsData = [
  {
    "name": "668",
    "x": 27,
    "y": 45,
    "width": 276,
    "height": 353,

  },
  {
    "name": "",
    "x": 312,
    "y": 45,
    "width": 133,
    "height": 353,
  }
]

const rooms = {};

// Räume + Snapzones erzeugen


function createLabel(name) {
  const label = document.createElement("div");
  label.className = "room-label";
  label.textContent = "Raum " + name;
  return label;
}

function createCounter() {
  const counter = document.createElement("div");
  counter.className = "room-counter";
  counter.textContent = "0 Objekt(e)";
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

  function getElementCenterOffset(e, el) {
    const elRect = el.getBoundingClientRect();
    const offsetX = e.clientX - (elRect.left + elRect.width / 2);
    const offsetY = e.clientY - (elRect.top + elRect.height / 2);
    //console.log("Center offset:", { offsetX, offsetY });
    return { offsetX, offsetY };
  }

  function startDragging(e) {
    e.preventDefault();
    dragging = true;
    el.style.cursor = "grabbing";
    console.log("Dragging started");

    const offsets = getElementCenterOffset(e, el);
    dragOffsetX = offsets.offsetX;
    dragOffsetY = offsets.offsetY;

    document.addEventListener("mousemove", onMouseMove);
    document.addEventListener("mouseup", onMouseUp);
  }

  function getMousePosRelativeToFloorplan(ev) {
    const floorplanRect = floorplan.getBoundingClientRect();
    let mouseX = ev.clientX - floorplanRect.left - el.offsetWidth / 2 - dragOffsetX;
    let mouseY = ev.clientY - floorplanRect.top - el.offsetHeight / 2 - dragOffsetY;
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
    const { x, y } = scaleAndClampPosition(mouseX, mouseY);
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
    console.log(`Added element to new room: ${newRoom.el.dataset.name}`);
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
const personDatabase = [
  { vorname: "Max", nachname: "Mustermann", alter: 30, rolle: "Entwickler", etage: 7 },
  { vorname: "Anna", nachname: "Müller", alter: 25, rolle: "Designer", etage: 6 }
];

const addPersonBtn = document.getElementById("addPersonBtn");
const personForm = document.getElementById("personForm");
const dynamicForm = document.getElementById("dynamicPersonForm");
const confirmPersonBtn = document.getElementById("confirmPersonBtn");
const existingPersonSelect = document.getElementById("existingPersonSelect");

const personSchema = [
  { label: "Vorname", key: "vorname", type: "string" },
  { label: "Nachname", key: "nachname", type: "string" },
  { label: "Alter", key: "alter", type: "integer" },
  { label: "Rolle", key: "rolle", type: "string" }
];

// Hilfsfunktion: Formular generieren
function generateForm(schema, formElement) {
  formElement.innerHTML = ""; // Formular leeren

  schema.forEach(field => {
    const label = document.createElement("label");
    label.textContent = field.label + ": ";

    const input = document.createElement("input");
    input.name = field.key;
    input.type = field.type === "integer" ? "number" : "text";
    input.required = true;

    label.appendChild(input);
    formElement.appendChild(label);
    formElement.appendChild(document.createElement("br"));
  });
}

// Bestehende Personen in Select füllen
function populateExistingPersonSelect() {
  existingPersonSelect.innerHTML = "";
  personDatabase.forEach((person, index) => {
    const option = document.createElement("option");
    option.value = index;
    option.textContent = `${person.vorname} ${person.nachname} (${person.rolle})`;
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

addPersonBtn.addEventListener("click", () => {
  personForm.style.display = "block";
  populateExistingPersonSelect();
  updateFormMode();
});

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
  const newPerson = collectPersonData(formData);

  if (!newPerson) {
    console.error("Neue Person konnte nicht erstellt werden – Felder unvollständig.");
    return;
  }

  personDatabase.push(newPerson);
  console.log("Neue Person zur Datenbank hinzugefügt:", newPerson);

  createPersonCircle(newPerson);
}

function collectPersonData(formData) {
  const newPerson = {};

  for (const field of personSchema) {
    let value = formData.get(field.key);

    if (!value) {
      alert(`Bitte das Feld "${field.label}" ausfüllen.`);
      console.warn(`Fehlendes Feld: ${field.key}`);
      return null;
    }

    if (field.type === "integer") {
      value = parseInt(value, 10);
      if (isNaN(value)) {
        console.warn(`Ungültige Ganzzahl für Feld ${field.key}:`, value);
        return null;
      }
    }

    newPerson[field.key] = value;
  }

  console.log("Daten für neue Person gesammelt:", newPerson);
  return newPerson;
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
}

function createCircleElement(attributes) {
  try {
    const circle = document.createElement("div");
    circle.classList.add("person-circle");
    Object.assign(circle.style, getCircleStyles());

    // Wichtig: erst Content setzen, damit Größe bekannt ist
    setCircleContent(circle, attributes);

    // Jetzt die Position setzen, damit es in der Mitte vom aktuellen Viewport ist
    setCirclePosition(circle);

    return circle;
  } catch (error) {
    console.error("Fehler beim Erzeugen des Kreises:", error);
  }
}

// Gibt die aktuelle Scroll-Position zurück (x und y)
function getScrollPosition() {
  return {
    x: window.scrollX || window.pageXOffset,
    y: window.scrollY || window.pageYOffset,
  };
}

// Gibt die aktuelle Größe des Viewports zurück
function getViewportSize() {
  return {
    width: window.innerWidth,
    height: window.innerHeight,
  };
}

// Berechnet die Mitte des Viewports relativ zum Dokument (inkl. Scroll)
function getViewportCenterPosition() {
  const scroll = getScrollPosition();
  const viewport = getViewportSize();

  return {
    x: scroll.x + viewport.width / 2,
    y: scroll.y + viewport.height / 2,
  };
}

// Setzt die Position des Elements auf die Mitte des Viewports
function setCirclePosition(circle) {
  const center = getViewportCenterPosition();

  // Damit es funktioniert, brauchen wir position: absolute oder fixed
  circle.style.position = "absolute";

  // Kreis zentrieren: Oben und Links sind die Koordinaten der Mitte minus halb so breit/hoch wie das Element
  const rect = circle.getBoundingClientRect();
  const width = rect.width || 50;  // Falls noch kein Width, Beispiel 50px
  const height = rect.height || 50;

  circle.style.left = `${center.x - width / 2}px`;
  circle.style.top = `${center.y - height / 2}px`;
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

function setCirclePosition(circle) {
  const viewport = getViewportSize();

  circle.style.position = "fixed"; // FIXED statt ABSOLUTE

  const rect = circle.getBoundingClientRect();
  const width = rect.width || 50;
  const height = rect.height || 50;

  circle.style.left = `${viewport.width / 2 - width / 2}px`;
  circle.style.top = `${viewport.height / 2 - height / 2}px`;
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
  try {
    circle.innerHTML = `
      <img src="https://scads.ai/wp-content/uploads/Bicanski_Andrej-_500x500-400x400.jpg" style="max-width: 64px; max-height: 64px;" />
      <strong>${my_escape(attributes.vorname)} ${my_escape(attributes.nachname)}</strong><br>
      Alter: ${my_escape(attributes.alter)}<br>
      Rolle: ${my_escape(attributes.rolle)}
    `;
  } catch (error) {
    console.error("Fehler beim Setzen des Inhalts für den Kreis:", error);
  }
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

    const menu = buildContextMenu(attributes);
    positionContextMenuAbsolute(circle, menu);
    floorplan.appendChild(menu); // WICHTIG: nicht circle.appendChild
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



function buildContextMenu(attributes) {
  try {
    const menu = document.createElement("div");
    menu.className = "context-menu";
    Object.assign(menu.style, getContextMenuStyles());

    menu.innerHTML = `
      <div><strong>${attributes.vorname} ${attributes.nachname}</strong></div>
      <div>Alter: ${attributes.alter}</div>
      <div>Rolle: ${attributes.rolle}</div>
    `;

    return menu;
  } catch (error) {
    console.error("Fehler beim Erstellen des Kontextmenüs:", error);
  }
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
const shapeSelector = document.getElementById("shapeSelector");
const shapeSelect = document.getElementById("shapeSelect");
const confirmAddBtn = document.getElementById("confirmAddBtn");

addBtn.addEventListener("click", () => {
  shapeSelector.style.display = "block";
});
confirmAddBtn.addEventListener("click", () => {
  selectedShape = shapeSelect.value;
  createObject(selectedShape);
  shapeSelector.style.display = "none";
});

function disableMouseWheelScrollAllowArrowKeys() {
  window.addEventListener('wheel', (event) => {
    event.preventDefault(); // Mausrad-Scrollen verhindern
  }, { passive: false });

  // Pfeiltasten bleiben unberührt und können scrollen
}



document.addEventListener("DOMContentLoaded", () => {
  const shapeSelector = document.getElementById("shapeSelector");
  const shapeSelect = document.getElementById("shapeSelect");
  const confirmAddBtn = document.getElementById("confirmAddBtn");

  const objectForm = document.getElementById("objectForm");
  const saveOptionsBtn = document.getElementById("saveOptionsBtn");
  const addBtn = document.getElementById("addBtn");

  addBtn.addEventListener("click", () => {
    shapeSelector.style.display = "block";
    objectForm.style.display = "none"; // Optionsfenster schließen, falls offen
  });

  confirmAddBtn.addEventListener("click", () => {
    const selectedShape = shapeSelect.value;

    createObject(selectedShape);

    shapeSelector.style.display = "none"; // Auswahlfenster schließen

    objectForm.style.display = "block";   // Optionsfenster öffnen
  });

  saveOptionsBtn.addEventListener("click", () => {
    const option1 = document.getElementById("option1").value;
    const option2 = document.getElementById("option2").value;
    const option3 = document.getElementById("option3").value;
    const option4 = document.getElementById("option4").value;

    console.log("Option 1:", option1);
    console.log("Option 2:", option2);
    console.log("Option 3:", option3);
    console.log("Option 4:", option4);

    objectForm.style.display = "none"; // Optionsfenster schließen

    // Eingabefelder leeren
    document.getElementById("option1").value = "";
    document.getElementById("option2").value = "";
    document.getElementById("option3").value = "";
    document.getElementById("option4").value = "";
  });

  // Dummy-Funktion zum Erstellen eines Objekts (bitte ersetzen)
  function createObject(shape) {
    console.log("Objekt erstellen:", shape);
  }
});

// Initial
createRooms();
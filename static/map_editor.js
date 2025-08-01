// TODO: Building-ID aus Gebäudeliste erstellen

let params;

try {
	params = new URLSearchParams(window.location.search);
} catch (e) {
	err("Fehler beim Parsen der URL:", e);
	params = new URLSearchParams(); // Leeres Fallback
}

let building_id_str = params.get("building_id");
let floor_str = params.get("floor");

let building_id = parseInt(building_id_str, 10);
let floor = parseInt(floor_str, 10);

function loadFloorplan(buildingId, floor) {
	if (typeof buildingId !== "number" || typeof floor !== "number") {
		err("loadFloorplan: buildingId und floor müssen Zahlen sein");
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

			rooms = data;

			renderAll();
			updateOutput();
		})
		.catch(function (e) {
			err("Fehler beim Laden des Floorplans:", e);
		});
}

function createGUID() {
	function getRandomHexDigit() {
		return Math.floor(Math.random() * 16).toString(16);
	}

	function getRandomHexDigits(count) {
		var digits = '';
		var i = 0;
		while (i < count) {
			digits += getRandomHexDigit();
			i++;
		}
		return digits;
	}

	function getRandomYDigit() {
		var y = Math.floor(Math.random() * 4) + 8; // 8, 9, A, or B
		return y.toString(16);
	}

	var part1 = getRandomHexDigits(8);
	var part2 = getRandomHexDigits(4);
	var part3 = '4' + getRandomHexDigits(3); // version 4
	var part4 = getRandomYDigit() + getRandomHexDigits(3); // variant
	var part5 = getRandomHexDigits(12);

	var guuid = part1 + '-' + part2 + '-' + part3 + '-' + part4 + '-' + part5;
	return guuid;
}

function getCallerInfo() {
	const stack = new Error().stack;
	const callerLine = stack.split("\n")[3]?.trim() || "unknown"; // [0]=Error, [1]=getCallerInfo, [2]=log, [3]=Aufrufer
	return callerLine.replace(/^at\s+/, '');
}

const log = (...args) => {
	console.log(`[${getCallerInfo()}]`, ...args);
};

const debug = (...args) => {
	console.debug(`[${getCallerInfo()}]`, ...args);
};

const warn = (...args) => {
	console.warn(`[${getCallerInfo()}]`, ...args);
};

const err = (...args) => {
	console.error(`[${getCallerInfo()}]`, ...args);
};

// State
let rooms = [];

let drawingRoom = null;
let selectedRoomId = null;
let dragData = null; // {type: 'room', id, offsetX, offsetY, element}
let resizeData = null; // {type, id, edge, startX, startY, startWidth, startHeight}
let global_room = null;
let zoomLevel = 1;
let focusedRoomId = null;
let focusedRoomCaret = null;
let mauszeigerAufRoom = false;

// IDs
let roomCounter = 1;

const output = document.getElementById('output');

function push_to_rooms (room) {
	rooms.push(room)
}

async function save_rooms() {
	for (var i = 0; i < rooms.length; i++) {
		await save_room(rooms[i]);
	}
}

async function save_room(room) {
	room.building_id = building_id;
	room.floor = floor;

	try {
		const response = await fetch("/api/save_or_update_room", {
			method: "POST",
			headers: {
				"Content-Type": "application/json"
			},
			body: JSON.stringify(room) // Kein Array mehr – nur ein Objekt!
		});

		const data = await response.json();

		if (data.status === "success") {
			const savedName = data.room_name || room.name;
			const savedId = data.room_id !== undefined ? ` (ID: ${data.room_id})` : "";
			console.log("Raum erfolgreich gespeichert:", savedName + savedId);
		} else {
			err("Fehler beim Speichern:", data.error || data);
		}
	} catch (e) {
		err("API Fehler:", e);
	}
}

// Helpers (jQuery-Version)
function createElement(tag, cls, parent) {
	//debug(`createElement: <${tag}> mit Klasse "${cls}"`);
	const $el = $('<' + tag + '/>');
	if (cls) $el.addClass(cls);
	if (parent) {
		//debug(`Element wird an Parent angehängt:`, parent);
		$(parent).append($el);
	}
	return $el[0]; // Falls du das native DOM-Element brauchst
}

function confirmDelete(msg) {
	warn(`Bestätigung erforderlich: ${msg}`);
	return window.confirm(msg);
}

function updateOutput() {
	try {
		//log('updateOutput: Räume werden exportiert...');
		const exportData = rooms.map(room => {
			return {
				name: room.name,
				x: Math.round(room.x),
				y: Math.round(room.y),
				width: Math.round(room.width),
				height: Math.round(room.height)
			};
		});

		//debug('Exportierte Daten:', exportData);
		$('#output').text(JSON.stringify(exportData, null, 2));
		//log('Export erfolgreich abgeschlossen.');
	} catch (e) {
		err('Fehler beim Export der Räume:', e);
	}
}
function getMousePos(evt) {
	const rect = container.getBoundingClientRect();
	const pos = {
		x: evt.clientX - rect.left,
		y: evt.clientY - rect.top
	};
	//debug('getMousePos:', pos);
	return pos;
}

function isInside(a, b) {
	const result = (
		a.x >= b.x &&
		a.y >= b.y &&
		a.x + a.width <= b.x + b.width &&
		a.y + a.height <= b.y + b.height
	);
	//debug('isInside:', { a, b, result });
	return result;
}

function delete_room(e, ask = true, this_room) {
	console.trace();
	if (e !== undefined && e !== null) {
		e.stopPropagation();
	}
	if (!this_room) return; // Falls kein Raum übergeben wurde, nichts tun

	if (ask && confirmDelete(`Raum "${this_room.name || this_room.id}" löschen?`) || ask == false) {
		warn(`Raum gelöscht: ${this_room.name || this_room.id}`);
		rooms = rooms.filter(r => r.id !== this_room.id);
		if (selectedRoomId === this_room.id) {
			selectedRoomId = null;
		}
		updateOutput();
		renderAll();

		fetch("/api/delete_room", {
			method: "POST",
			headers: {
				"Content-Type": "application/json"
			},
			body: JSON.stringify({ name: this_room.name })
		})
			.then(response => response.json())
			.then(data => {
				if (data.status) {
					log("Raum gelöscht aus DB: " + this_room.name);
				} else {
					err("Fehler beim Löschen:", data.error || data);
				}
			})
			.catch(e=> {
				err("API Fehler beim Löschen:", err);
			});

	} else {
		log(`NOT DELETING: ${this_room}`)
	}
}

// Raum erstellen
async function createRoomElement(room) {
	//log(`Raum-Element wird erstellt:`, room);
	const $el = $('<div class="room"></div>').appendTo($('#container'));
	$el.css({
		left: room.x + 'px',
		top: room.y + 'px',
		width: room.width + 'px',
		height: room.height + 'px',
		borderColor: 'rgba(0,0,255,0.6)',
		zIndex: 1 // Alle Räume auf gleicher Ebene!
	});
	$el.attr('data-id', room.id);

	// Name-Eingabe
	const $nameInput = $('<input type="text" class="name-input" title="Raumname">')
		.val(room.name)
		.appendTo($el)
		.on('mousedown', function(e) {
			// Hier merken wir uns das Feld, bevor ein renderAll() kommt!
			focusedRoomId = room.id;
			// Caret-Position merken (am Anfang des Klicks)
			focusedRoomCaret = this.selectionStart;
			e.stopPropagation();
		})
		.on('focus', function () {
			// Nur setzen, wenn noch nicht gesetzt (z.B. nach Tab oder Programmatisch)
			if (!focusedRoomId) focusedRoomId = room.id;
			if (!focusedRoomCaret) focusedRoomCaret = this.selectionStart;
		})
		.on('input', function () {
			room.name = $(this).val();
			if(room.name) {
				debug(`Raumname geändert: ${room.name}`);

				save_room(room);

				focusedRoomCaret = this.selectionStart;
			}
		})
		.on('blur', function () {
			focusedRoomId = null;
			focusedRoomCaret = null;
		});

	global_room = room;

	// Löschen-Button
	const $delBtn = $('<div class="delete-btn" title="Raum löschen">×</div>')
		.appendTo($el)
		.on('click', function(e) {
			delete_room(e, true, room); // <-- Das aktuelle Raum-Objekt übergeben!
		});

	// Drag/Resize
	await enableDragResize($el, 'room', room);
	//debug('Drag/Resize für Raum aktiviert');

	applyInvertFilterToElements(theme);

	return $el[0];
}

async function getResizeEdge(e, rect) {
	try {
		if (!e || !rect) {
			err('getResizeEdge: Event or rect is undefined');
			return null;
		}

		const margin = 8; // Bereich in Pixeln, in dem Resize erkannt wird
		const x = e.clientX - rect.left;
		const y = e.clientY - rect.top;
		const right = rect.width;
		const bottom = rect.height;

		log(`Mouse position relative to element: x=${x}, y=${y}`);
		log(`Element size: width=${right}, height=${bottom}`);

		let edge = '';

		if (x > right - margin) {
			edge += 'r';  // rechte Kante
			log('Resize edge detected: right');
		} else if (x < margin) {
			edge += 'l';  // linke Kante
			log('Resize edge detected: left');
		}

		if (y > bottom - margin) {
			edge += 'b';  // untere Kante
			log('Resize edge detected: bottom');
		} else if (y < margin) {
			edge += 't';  // obere Kante
			log('Resize edge detected: top');
		}

		if (!edge) {
			log('No resize edge detected');
			return null;
		} else {
			await save_rooms();
		}

		log(`Final resize edge string: "${edge}"`);
		return edge;
	} catch (e) {
		err('Error in getResizeEdge:', e);
		return null;
	}
}
function getScaleFactor() {
	const container = document.getElementById('container');
	const style = window.getComputedStyle(container);
	const transform = style.transform || style.webkitTransform;

	if (!transform || transform === 'none') return 1;

	const match = transform.match(/^matrix\((.+)\)$/);
	if (match) {
		const values = match[1].split(', ');
		const scaleX = parseFloat(values[0]);
		return isNaN(scaleX) ? 1 : scaleX;
	}

	return 1;
}

function getMouseRelativeToContainer(event) {
	const container = document.getElementById('container');
	const rect = container.getBoundingClientRect();
	const scale = getScaleFactor();

	return {
		x: (event.clientX - rect.left) / scale,
		y: (event.clientY - rect.top) / scale
	};
}

async function enableDragResize($el, type, obj, parentRoom = null) {
	$el.on('mousedown', async function (e) {
		// Hole das geklickte Raum-Element nach oben (letztes Kind im Container)
		container.appendChild(this);

		const topEl = document.elementFromPoint(e.clientX, e.clientY);
		if (topEl !== this) return; // Nur das oberste Element darf Drag/Resize starten

		if(e.target.type != "text") {
			const $target = $(e.target);
			if (
				$target.hasClass('delete-btn') ||
				$target.is('input') ||
				$target.is('select')
			) {
				debug('Drag/Resize übersprungen wegen UI-Element-Klick');
				return;
			}

			e.preventDefault();

			const rect = this.getBoundingClientRect();
			const edge = await getResizeEdge(e, rect);
			const start = getMouseRelativeToContainer(e);

			window.dragData = null;
			window.resizeData = null;

			let dragDataLocal = {
				type,
				id: obj.id,
				element: this,
				startX: start.x,
				startY: start.y,
				origX: obj.x,
				origY: obj.y,
				origWidth: obj.width,
				origHeight: obj.height,
				edge,
				parentRoom
			};

			if (edge) {
				log(`Resize gestartet (${type}): ID=${obj.id}, Edge=${edge}`);
				window.resizeData = dragDataLocal;
			} else {
				log(`Drag gestartet (${type}): ID=${obj.id}`);
				window.dragData = dragDataLocal;
			}

			$(document).on('mousemove.dragResize', function (moveEvent) {
				const pos = getMouseRelativeToContainer(moveEvent);

				if (window.dragData) {
					const dx = pos.x - window.dragData.startX;
					const dy = pos.y - window.dragData.startY;

					obj.x = window.dragData.origX + dx;
					obj.y = window.dragData.origY + dy;

					$(window.dragData.element).css({
						left: obj.x + 'px',
						top: obj.y + 'px'
					});

				} else if (window.resizeData) {
					const dx = pos.x - window.resizeData.startX;
					const dy = pos.y - window.resizeData.startY;

					const edge = window.resizeData.edge;

					if (edge.includes('right')) {
						obj.width = window.resizeData.origWidth + dx;
					}
					if (edge.includes('bottom')) {
						obj.height = window.resizeData.origHeight + dy;
					}
					if (edge.includes('left')) {
						obj.x = window.resizeData.origX + dx;
						obj.width = window.resizeData.origWidth - dx;
					}
					if (edge.includes('top')) {
						obj.y = window.resizeData.origY + dy;
						obj.height = window.resizeData.origHeight - dy;
					}

					$(window.resizeData.element).css({
						left: obj.x + 'px',
						top: obj.y + 'px',
						width: obj.width + 'px',
						height: obj.height + 'px'
					});
				}
			});

			$(document).on('mouseup.dragResize', function (e) {
				// Wenn aktuell ein Input-Feld den Fokus hat, Drag/Resize NICHT beenden!
				if (document.activeElement && document.activeElement.tagName === "INPUT") return;

				if (window.dragData) {
					log(`Drag beendet (${type}): ID=${window.dragData.id}`);
					window.dragData = null;

					save_rooms();
				}
				if (window.resizeData) {
					log(`Resize beendet (${type}): ID=${window.resizeData.id}`);
					window.resizeData = null;
				}
				$(document).off('.dragResize');
			});
		} else {
			e.stopPropagation();
		}
	});
}

function cancel_drag(e) {
	// Nur Escape-Taste oder Mouseup außerhalb von Input-Feldern
	if (e.type === 'keydown' && e.key === 'Escape') {
		//log("cancel_drag, event:", e)
		if (window.dragData || window.resizeData) {
			log('Drag/Resize beendet (Escape gedrückt)');
		}
		window.dragData = null;
		window.resizeData = null;
	}

	if (e.type === 'mouseup') {
		//log("cancel_drag, event:", e)
		// Wenn aktuell ein Input-Feld den Fokus hat, nichts tun!
		if (document.activeElement && document.activeElement.tagName === "INPUT") return;
		window.dragData = null;
		window.resizeData = null;
	}

}

$(document).on('keydown', cancel_drag).on('mouseup', cancel_drag);

$(window).on('mousemove', function (e) {
	if (window.dragData) {
		e.preventDefault();
		const dx = e.clientX - window.dragData.startX;
		const dy = e.clientY - window.dragData.startY;

		if (window.dragData.type === 'room') {
			// Move room
			$(window.dragData.element).css({
				left: (window.dragData.origX + dx) + 'px',
				top: (window.dragData.origY + dy) + 'px',
				width: window.dragData.origWidth + 'px',
				height: window.dragData.origHeight + 'px'
			});

			const room = rooms.find(r => r.id === window.dragData.id);
			if (room) {
				room.x = window.dragData.origX + dx;
				room.y = window.dragData.origY + dy;
				//debug(`Raum verschoben → x:${room.x}, y:${room.y}`);
			}
		}

	} else if (window.resizeData) {
		e.preventDefault();
		const dx = e.clientX - window.resizeData.startX;
		const dy = e.clientY - window.resizeData.startY;

		if (window.resizeData.type === 'room') {
			const room = rooms.find(r => r.id === window.resizeData.id);
			if (!room) return;

			let x = room.x;
			let y = room.y;
			let w = window.resizeData.origWidth;
			let h = window.resizeData.origHeight;

			if (window.resizeData.edge.includes('r')) w = Math.max(20, w + dx);
			if (window.resizeData.edge.includes('b')) h = Math.max(20, h + dy);
			if (window.resizeData.edge.includes('l')) {
				w = Math.max(20, w - dx);
				x = window.resizeData.origX + dx;
			}
			if (window.resizeData.edge.includes('t')) {
				h = Math.max(20, h - dy);
				y = window.resizeData.origY + dy;
			}

			x = Math.max(0, x);
			y = Math.max(0, y);
			if (x + w > container.clientWidth) w = container.clientWidth - x;
			if (y + h > container.clientHeight) h = container.clientHeight - y;

			room.x = x;
			room.y = y;
			room.width = w;
			room.height = h;

			debug(`Raum resized → x:${x}, y:${y}, w:${w}, h:${h}`);

			renderAll();
			updateOutput();

		}
	}
});
// Drawing new rooms (mousedown)
$(container).on('mousedown', function (e) {
	if(e.target.id == "container") {
		if (e.button === 0 && e.target.id == "container") {
			const pos = getMousePos(e);
			drawingRoom = { x: pos.x, y: pos.y, width: 0, height: 0 };
			//log(`Start drawing room at (${pos.x}, ${pos.y})`);
			drawTempRect('room', drawingRoom);
		}
	}
});

// Mousemove for drawing
$(container).on('mousemove', function (e) {
	if (drawingRoom) {
		const pos = getMousePos(e);
		drawingRoom.width = Math.max(1, pos.x - drawingRoom.x);
		drawingRoom.height = Math.max(1, pos.y - drawingRoom.y);
		drawTempRect('room', drawingRoom);
	}
});

// Mouseup to finalize drawing
$(window).on('mouseup', function (e) {
	if (drawingRoom) {
		if (drawingRoom.width > 5 && drawingRoom.height > 5) {
			const newRoom = {
				id: 'r' + roomCounter++,
				name: '',
				x: drawingRoom.x,
				y: drawingRoom.y,
				width: drawingRoom.width,
				height: drawingRoom.height,
				guid: createGUID()
			};
			push_to_rooms(newRoom);
			selectedRoomId = newRoom.id;
			log(`Neuer Raum erstellt: ID=${newRoom.id}, Position=(${newRoom.x},${newRoom.y}), Größe=(${newRoom.width}x${newRoom.height})`);

			drawingRoom = null;
		} else {
			warn('Gezeichneter Raum zu klein, wird ignoriert.');
		}

		removeTempRects();
		renderAll();
		updateOutput();

	}
}); 

function drawTempRect(type, rect, parentRoom = null) {
	//log(`drawTempRect called for type "${type}" with rect:`, rect, parentRoom ? `in room ${parentRoom.id}` : 'in container');
	removeTempRects();

	const temp = createElement('div', 'room', parentRoom ? null : container);

	if (parentRoom) {
		// Erst Element mit id suchen
		let parentEl = document.getElementById(parentRoom.id);

		// Falls nicht gefunden, Element mit data-id suchen
		if (!parentEl) {
			parentEl = document.querySelector(`[data-id="${parentRoom.id}"]`);
		}

		if (!parentEl) {
			err(`Parent room element mit id oder data-id "${parentRoom.id}" nicht gefunden! Temp rect kann nicht angehängt werden.`);
			return;
		}

		parentEl.appendChild(temp);
		//debug(`Temp ${type} rect angehängt an Raum ${parentRoom.id}`);
	} else {
		//debug(`Temp ${type} rect angehängt an Container`);
	}

	$(temp).css({
		left: rect.x + 'px',
		top: rect.y + 'px',
		width: rect.width + 'px',
		height: rect.height + 'px',
		borderColor: 'rgba(0,0,255,0.8)',
		backgroundColor: 'rgba(0,0,255,0.3)'
	}).attr('id', 'temp-' + type);

	//debug(`Temp ${type} rect gestylt und id "temp-${type}" gesetzt.`);
}

function removeTempRects() {
	['temp-room'].forEach(id => {
		const el = document.getElementById(id);
		if (el) {
			el.remove();
			//log(`Temp rect mit id "${id}" entfernt.`);
		} else {
			//debug(`Kein Temp rect mit id "${id}" zum Entfernen gefunden.`);
		}
	});
}

async function renderAll() {
	container.innerHTML = '';

	// Räume nach ID sortieren (höchste ID zuletzt)
	const sortedRooms = rooms.slice().sort((a, b) => a.id.localeCompare(b.id));
	for (const room of sortedRooms) {
		const el = await createRoomElement(room);
		if (!el) continue;
		container.appendChild(el);
	}

	updateOutput();
	// Fokus wiederherstellen ...
}

function startDrawRoom(e) {
	drawingRoom = null;
}

// Deselect room on container click
container.addEventListener('click', async e => {
	// Wenn ein Input-Feld geklickt wurde, NICHT rendern!
	if (e.target && e.target.classList.contains('name-input')) return;

	if(selectedRoomId) {
		log(`Container geklickt: Raum (${selectedRoomId}) abwählen.`);
		selectedRoomId = null;
	} else {
		log(`Container geklickt: Kein Raum selektiert`)
		startDrawRoom(e);
	}
	await renderAll();
});

async function import_text() {
	if (!$("#import").length) {
		err("#import element nicht gefunden");
		return;
	}

	var text = $("#import").val()

	if (text.match(/^\s*$/)) {
		return;
	}

	var parsed = null;

	try {
		parsed = JSON.parse(text);
	} catch (e) {
		err(`import_text: error parsing ${text}: ${e}`);
		return;
	}

	for (var i = 0; i < parsed.length; i++) {
		var this_room = parsed[i];

		if (rooms.some(room => room.name === this_room.name)) {
			//delete_room(null, false, this_room);

			document.querySelectorAll('.room').forEach(roomEl => {
				const input = roomEl.querySelector('.name-input');
				if (input && input.value === this_room.name) {
					roomEl.remove();
					log("removing:", this_room.name, roomEl);
				}
			});

			rooms = rooms.filter(room => room.name !== this_room.name);
		}

		drawingRoom = {
			x: this_room.x,
			y: this_room.y,
			width: this_room.width,
			height: this_room.height
		};

		log(`Import: Start drawing room at (${this_room.x}, ${this_room.y}) with width = ${this_room.width} and height = ${this_room.height}`);
		drawTempRect('room', drawingRoom);

		drawingRoom = null;

		const newRoom = {
			id: `r${roomCounter++}`,
			name: this_room.name,
			x: this_room.x,
			y: this_room.y,
			width: this_room.width,
			height: this_room.height,
			guid: this_room.guid ? this_room.guid : createGUID() // <-- guid übernehmen!
		};

		push_to_rooms(newRoom);
		await save_room(newRoom);
	}

	// Nach dem Import: Räume aus DB neu laden und rendern!
	await new Promise(resolve => {
		loadFloorplan(building_id, floor);
		setTimeout(resolve, 500); // Kurze Pause, damit DB speichern durch ist
	});
}

document.addEventListener('mousemove', (event) => {
	const element = document.elementFromPoint(event.clientX, event.clientY);
	if (!element) {
		mauszeigerAufRoom = false;
		return;
	}
	// Prüfen, ob element oder ein Elternteil die Klasse 'room' hat
	mauszeigerAufRoom = element.closest('.room') !== null;
});

if(!isNaN(building_id) && !isNaN(floor)) {
	loadFloorplan(building_id, floor);
}
window.import_text = import_text;

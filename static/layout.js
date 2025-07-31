var theme = "light";
document.documentElement.classList.remove('no-js');

function applyInvertFilterToElements(mode) {
	const floorplan = document.getElementById('floorplan');
	const backgroundImage = document.getElementById('backgroundImage');

	if (floorplan) {
		floorplan.style.filter = (mode === 'dark') ? 'invert(1)' : '';
	}
	if (backgroundImage) {
		backgroundImage.style.filter = (mode === 'dark') ? 'invert(1)' : '';
	}

	const inputs = document.querySelectorAll('input, textarea, button');
	inputs.forEach(function(element) {
		if (mode === 'dark') {
			element.style.backgroundColor = '#1a202c';  // dunkelgrau
			element.style.color = '#f7fafc';            // hell
			element.style.borderColor = '#2d3748';     // Rahmen
		} else {
			element.style.backgroundColor = '';
			element.style.color = '';
			element.style.borderColor = '';
		}
	});

	// Speziell für Select-Elemente: 
	const selects = document.querySelectorAll('select');
	selects.forEach(function(element) {
		if (mode === 'dark') {
			element.style.backgroundColor = '#1a202c';
			element.style.color = '#f7fafc';
			element.style.borderColor = '#2d3748';

		} else {
			element.style.backgroundColor = '';
			element.style.color = '';
			element.style.borderColor = '';
		}
	});


	if(mode === "dark") {
		$("select").css("background-color", "1a202c");
		$("input, textarea, select, button").css("border", "1px solid #3daee9");
		$("#objectForm, #personForm").css("background-color", "black");
		$(".optionContainer,.context-menu").css("color", "white").css("background-color", "black").css("filter", "invert(1)");
		$(".person-circle>img").css("filter", "invert(1)")
	} else {
		$("select").css("background-color", "unset");
		$("input, textarea, select, button").css("border", "1px solid black");
		$("#objectForm, #personForm").css("background-color", "white");
		$(".optionContainer,.context-menu").css("color", "black").css("background-color", "white").css("filter", "unset");
		$(".person-circle>img").css("filter", "unset")
	}


	$(".delete-entry").css("background-color", "#e74c3c");
}

const SPinput = document.getElementById('sidebarSearch');
if(SPinput) {
	SPinput.addEventListener('input', () => {
		if (SPinput.value.trim() !== '') {
			SPinput.style.color = 'white';
		} else {
			SPinput.style.color = ''; // zurücksetzen
		}
	});
}

document.addEventListener('DOMContentLoaded', function () {
	try {
		const html = document.documentElement;
		const toast = document.getElementById('toast');
		const sidebar = document.getElementById('sidebar');
		const main = document.querySelector('main');
		const toggleThemeBtn = document.getElementById('toggleThemeBtn');
		const logoutBtn = document.getElementById('logoutBtn');

		if (!main) {
			console.error('Element <main> nicht gefunden.');
			return;
		}
		if (!toggleThemeBtn) {
			return;
		}
		if (!toast) {
			console.error('Toast-Element nicht gefunden.');
			return;
		}
		if (!sidebar) {
			console.error('Sidebar nicht gefunden.');
			// Weiter machen, da evtl. nicht kritisch
		}
		if (!logoutBtn) {
			console.warn('Logout-Button nicht gefunden.');
			// Weiter machen, falls kein Logout vorhanden
		}

		if (sidebar) {
			sidebar.classList.remove('invisible');
			sidebar.classList.add('transition-transform', 'duration-300');
		}

		function applyMainBgClass(mode) {
			if (mode === 'dark') {
				main.classList.add('bg-gray-900', 'text-gray-100');
				main.classList.remove('bg-white', 'text-gray-900');
			} else {
				main.classList.add('bg-white', 'text-gray-900');
				main.classList.remove('bg-gray-900', 'text-gray-100');
			}
		}

		function showToast(message) {
			toast.textContent = message;
			toast.classList.remove('hidden');
			toast.style.opacity = '1';
			setTimeout(() => {
				toast.style.opacity = '0';
				setTimeout(() => {
					toast.classList.add('hidden');
				}, 300);
			}, 2500);
		}

		function updateThemeButtonIcon() {
			const isDark = html.classList.contains('dark');
			toggleThemeBtn.textContent = isDark ? "☀️ Light Mode" : "🌙 Dark Mode";
		}

		function applyTheme(mode) {
			theme = mode;
			if (mode === 'dark') {
				html.classList.add('dark');
				localStorage.setItem('theme', 'dark');
				applyMainBgClass('dark');
				applyInvertFilterToElements('dark');
				updateThemeButtonIcon();
				showToast("🌙 Dark Mode aktiviert");
			} else {
				html.classList.remove('dark');
				localStorage.setItem('theme', 'light');
				applyMainBgClass('light');
				applyInvertFilterToElements('light');
				updateThemeButtonIcon();
				showToast("☀️ Light Mode aktiviert");
			}
		}

		const storedTheme = localStorage.getItem('theme');
		const prefersDark = window.matchMedia('(prefers-color-scheme: dark)').matches;

		if (storedTheme) {
			applyTheme(storedTheme);
		} else if (prefersDark) {
			applyTheme('dark');
		} else {
			applyTheme('light');
		}

		toggleThemeBtn.addEventListener('click', function () {
			const isDark = html.classList.contains('dark');
			applyTheme(isDark ? 'light' : 'dark');
		});

		if (logoutBtn) {
			logoutBtn.addEventListener('click', function () {
				showToast("🚪 Ausgeloggt");
				// window.location.href = "/logout"; // Optional aktivieren
			});
		}
	} catch (error) {
		console.error('Fehler im Theme-Script:', error);
	}
});

const resultsBox = document.getElementById('sidebarSearchResults');

let timeout = null;

if (SPinput && resultsBox) {
    let timeout = null;
    let selectedIndex = -1;

    // Hilfsfunktion, um zu prüfen, ob die Taste "normales" Tippen ist
    function isNormalKey(event) {
        // Keine Modifikatortasten gedrückt und kein Steuerzeichen
        if (event.ctrlKey || event.altKey || event.metaKey) {
            return false;
        }
        // Ignoriere Steuer- und Funktionstasten, nur Buchstaben, Zahlen und Zeichen erlauben
        const key = event.key;

        // Liste der erlaubten Zeichen (Buchstaben, Zahlen, Satzzeichen, Leerzeichen)
        // Einfacher Check: key.length === 1 -> sichtbares Zeichen (z.B. 'a', '1', '.', ' ')
        return key.length === 1;
    }

    // Hilfsfunktion zur Aktualisierung der Auswahl in der Ergebnisliste
    function updateSelection(index) {
        const items = resultsBox.querySelectorAll('li');
        if (items.length === 0) {
            selectedIndex = -1;
            return;
        }

        // Auswahl index begrenzen
        if (index < 0) {
            index = items.length - 1;
        }
        if (index >= items.length) {
            index = 0;
        }

        // Alle entfernen
        items.forEach((item, i) => {
            if (i === index) {
                item.classList.add('bg-blue-500', 'text-white');
                item.scrollIntoView({ block: 'nearest' });
            } else {
                item.classList.remove('bg-blue-500', 'text-white');
            }
        });
        selectedIndex = index;
    }

    SPinput.addEventListener('keydown', (event) => {
        if (resultsBox.classList.contains('hidden')) {
            // Falls Ergebnisbox versteckt ist, nichts tun außer Suche triggern bei normalen Zeichen
            return;
        }

        // Pfeiltasten und Enter behandeln
        switch (event.key) {
            case 'ArrowDown':
                event.preventDefault();
                updateSelection(selectedIndex + 1);
                break;
            case 'ArrowUp':
                event.preventDefault();
                updateSelection(selectedIndex - 1);
                break;
            case 'Enter':
                if (selectedIndex >= 0) {
                    event.preventDefault();
                    const items = resultsBox.querySelectorAll('li');
                    if (items[selectedIndex]) {
                        const url = items[selectedIndex].getAttribute('data-url');
                        if (url) {
                            window.location.href = url;
                        }
                    }
                }
                break;
            case 'Escape':
                resultsBox.classList.add('hidden');
                selectedIndex = -1;
                break;
        }
    });

    SPinput.addEventListener('input', (event) => {
        // Nur starten wenn normaler Tastendruck (kein Ctrl/Alt/Meta)
        // input Event hat kein event.key, daher diese Prüfung hier nicht möglich
        // Alternativ: Suche in keydown starten, aber wir wollen debounce – daher erlauben wir hier normal input

        clearTimeout(timeout);
        const query = SPinput.value.trim();

        if (query.length === 0) {
            resultsBox.innerHTML = '';
            resultsBox.classList.add('hidden');
            selectedIndex = -1;
            return;
        }

        timeout = setTimeout(() => {
            fetch(`/search?q=${encodeURIComponent(query)}`)
                .then(response => {
                    if (!response.ok) {
                        throw new Error('Netzwerkfehler beim Abrufen der Suche');
                    }
                    return response.json();
                })
                .then(results => {
                    resultsBox.innerHTML = '';
                    selectedIndex = -1;
                    if (Array.isArray(results) && results.length > 0) {
                        results.forEach(result => {
                            const li = document.createElement('li');
                            li.textContent = result.label;
                            li.className = 'px-3 py-2 hover:bg-gray-200 cursor-pointer';
                            li.setAttribute('data-url', result.url);
                            li.onclick = () => {
                                window.location.href = result.url;
                            };
                            li.addEventListener('mouseenter', () => {
                                // Maus-Hover aktualisiert Auswahl
                                const items = resultsBox.querySelectorAll('li');
                                items.forEach((item) => {
                                    item.classList.remove('bg-blue-500', 'text-white');
                                });
                                li.classList.add('bg-blue-500', 'text-white');
                                selectedIndex = Array.from(items).indexOf(li);
                            });
                            resultsBox.appendChild(li);
                        });
                        resultsBox.classList.remove('hidden');
                    } else {
                        resultsBox.classList.add('hidden');
                    }
                })
                .catch(error => {
                    console.error('Fehler bei der Suche:', error);
                    resultsBox.innerHTML = '';
                    resultsBox.classList.add('hidden');
                    selectedIndex = -1;
                });
        }, 200);
    });

    document.addEventListener('click', (event) => {
        if (!resultsBox.contains(event.target) && event.target !== SPinput) {
            resultsBox.classList.add('hidden');
            selectedIndex = -1;
        }
    });
}



document.addEventListener('keydown', function(e) {
	const searchField = document.getElementById('sidebarSearch');

	if (!searchField) return;

	// Ignoriere Eingabe, wenn gerade ein Eingabefeld oder contentEditable aktiv ist
	const active = document.activeElement;
	if (
		active &&
		(active.tagName === 'INPUT' ||
			active.tagName === 'TEXTAREA' ||
			active.tagName === 'SELECT' ||
			active.isContentEditable)
	) return;

	// Nur Buchstaben/Zahlen/Sonderzeichen verarbeiten (nicht z. B. Shift, Alt usw.)
	if (e.key.length === 1) {
		// Fokus setzen
		if (document.activeElement !== searchField) {
			searchField.focus();
			// Optional: Alles markieren oder Cursor ans Ende setzen
			searchField.setSelectionRange(searchField.value.length, searchField.value.length);
		}

		// Zeichen ans Ende anhängen (wie echte Eingabe)
		const value = searchField.value;
		const start = searchField.selectionStart;
		const end = searchField.selectionEnd;

		const newValue = value.slice(0, start) + e.key + value.slice(end);
		searchField.value = newValue;

		// Cursor aktualisieren
		const cursorPos = start + 1;
		searchField.setSelectionRange(cursorPos, cursorPos);

		// Events auslösen
		searchField.dispatchEvent(new Event('input', { bubbles: true }));
		searchField.dispatchEvent(new Event('change', { bubbles: true }));

		// Standardverhalten verhindern (z. B. Scrollen bei Leertaste)
		e.preventDefault();
	}
});

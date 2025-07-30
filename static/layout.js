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
			console.error('Button zum Umschalten des Themes (toggleThemeBtn) nicht gefunden.');
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

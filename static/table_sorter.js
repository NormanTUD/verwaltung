(function() {
	let originalTableData = new WeakMap(); 
	const arrows = { none: '↔', asc: '↑', desc: '↓' };

	function compare(a, b, type) {
		a = a.trim(); b = b.trim();
		if (type === 'num') {
			let na = parseFloat(a.replace(',', '.'));
			let nb = parseFloat(b.replace(',', '.'));
			return na - nb;
		} else {
			return a.localeCompare(b);
		}
	}

	function detectType(val) {
		return /^-?\d+([.,]\d+)?$/.test(val.trim()) ? 'num' : 'text';
	}

	function getCellValue(row, idx) {
		const td = $(row).children('td').eq(idx);
		const input = td.find('input[type=text], select').first();
		return input.length ? input.val() || input.find('option:selected').text() : td.text();
	}

	function applySorting($table, colIndex, direction) {
		const $tbody = $table.find('tbody');
		let rows = originalTableData.get($table)['originalRows'];

		if (direction === 'asc' || direction === 'desc') {
			const type = detectType(getCellValue(rows[0], colIndex));
			rows = rows.slice().sort((a, b) => {
				const va = getCellValue(a, colIndex);
				const vb = getCellValue(b, colIndex);
				return direction === 'asc' ? compare(va, vb, type) : compare(vb, va, type);
			});
		}

		$tbody.empty().append(rows);
		// Update current sorted rows so subsequent sorts always sort original data
		originalTableData.get($table)['currentRows'] = rows;
	}

	function initSorting() {
		$('table').each(function() {
			const $table = $(this);
			const $thead = $table.find('thead');
			const $tbody = $table.find('tbody');
			if (!$thead.length || !$tbody.length) return;

			const $headers = $thead.find('th');
			if (!$headers.length) return;

			let rows = $tbody.children('tr').get();
			if (!rows.length) return;

			// Speichere Original Reihenfolge komplett
			originalTableData.set($table, { originalRows: rows, currentRows: rows });

			$headers.each(function(i) {
				const $th = $(this);
				if ($th.text().trim() === '') return;

				$th.css('cursor', 'pointer');

				// Pfeil-Spanne nur hinzufügen, nicht Text ersetzen
				if ($th.find('.sort-arrow').length === 0) {
					$th.append(' ').append($('<span>').addClass('sort-arrow').text(arrows.none));
				}

				let state = 'none';

				$th.off('click').on('click', function() {
					try {
						// Alle Pfeile auf neutral setzen
						$headers.find('.sort-arrow').text(arrows.none);

						// Status wechseln
						if (state === 'none') {
							state = 'asc';
						} else if (state === 'asc') {
							state = 'desc';
						} else {
							state = 'none';
						}

						// Pfeil aktualisieren
						$th.find('.sort-arrow').text(arrows[state]);

						applySorting($table, i, state);
					} catch (e) {
						error("Sorting error: " + e.message);
					}
				});
			});
		});
	}

	$(document).ready(function() {
		try {
			initSorting();
		} catch (e) {
			error("Fehler beim Initialisieren der Sortierung: " + e.message);
		}
	});
})();

function showSpinner(text) {
	if (document.getElementById('___spinner_overlay')) return;

	// Overlay
	var overlay = document.createElement('div');
	overlay.id = '___spinner_overlay';
	overlay.style.position = 'fixed';
	overlay.style.top = 0;
	overlay.style.left = 0;
	overlay.style.width = '100%';
	overlay.style.height = '100%';
	overlay.style.backgroundColor = 'rgba(0, 0, 0, 0.7)';
	overlay.style.zIndex = 99999;
	overlay.style.display = 'flex';
	overlay.style.flexDirection = 'column';
	overlay.style.justifyContent = 'center';
	overlay.style.alignItems = 'center';
	overlay.style.color = 'white';
	overlay.style.fontSize = '20px';
	overlay.style.fontFamily = 'sans-serif';

	// Spinner-Element (SVG für garantierte Animation)
	var spinner = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
	spinner.setAttribute('width', '60');
	spinner.setAttribute('height', '60');
	spinner.setAttribute('viewBox', '0 0 100 100');
	spinner.style.marginBottom = '20px';

	var circle = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
	circle.setAttribute('cx', '50');
	circle.setAttribute('cy', '50');
	circle.setAttribute('r', '40');
	circle.setAttribute('stroke', 'white');
	circle.setAttribute('stroke-width', '8');
	circle.setAttribute('fill', 'none');
	circle.setAttribute('stroke-linecap', 'round');
	circle.setAttribute('stroke-dasharray', '200');
	circle.setAttribute('stroke-dashoffset', '100');

	var animate = document.createElementNS('http://www.w3.org/2000/svg', 'animateTransform');
	animate.setAttribute('attributeName', 'transform');
	animate.setAttribute('attributeType', 'XML');
	animate.setAttribute('type', 'rotate');
	animate.setAttribute('from', '0 50 50');
	animate.setAttribute('to', '360 50 50');
	animate.setAttribute('dur', '1s');
	animate.setAttribute('repeatCount', 'indefinite');

	circle.appendChild(animate);
	spinner.appendChild(circle);

	// Text
	var message = document.createElement('div');
	message.textContent = text || 'Lade Seite...';

	overlay.appendChild(spinner);
	overlay.appendChild(message);
	document.body.appendChild(overlay);
}

function removeSpinner() {
	var overlay = document.getElementById('___spinner_overlay');
	if (overlay) overlay.remove();
}

function getOrCreateMessageContainer() {
	var container = document.getElementById('js-notify-container');
	if (!container) {
		container = document.createElement('div');
		container.id = 'js-notify-container';
		container.style.position = 'fixed';
		container.style.top = '20px';
		container.style.right = '20px';
		container.style.zIndex = '9999';
		container.style.display = 'flex';
		container.style.flexDirection = 'column';
		container.style.alignItems = 'flex-end';
		container.style.gap = '10px';
		document.body.appendChild(container);
	}
	return container;
}

// Message erzeugen
function showMessage(type, text) {
	var colors = {
		success: '#2ecc71',
		error:   '#e74c3c',
		info:    '#3498db',
		warning: '#f1c40f'
	};

	var bgColor = colors[type] || '#333';

	var container = getOrCreateMessageContainer();

	var message = document.createElement('div');
	message.textContent = text;
	message.style.position = 'relative';
	message.style.minWidth = '200px';
	message.style.maxWidth = '400px';
	message.style.padding = '12px 16px';
	message.style.borderRadius = '6px';
	message.style.boxShadow = '0 2px 10px rgba(0,0,0,0.2)';
	message.style.color = '#fff';
	message.style.fontFamily = 'sans-serif';
	message.style.fontSize = '14px';
	message.style.cursor = 'pointer';
	message.style.backgroundColor = bgColor;
	message.style.opacity = '0';
	message.style.transition = 'opacity 0.2s ease';
	message.style.overflow = 'hidden';

	// Copy Button
	var copyBtn = document.createElement('button');
	copyBtn.textContent = 'Copy';
	copyBtn.style.position = 'absolute';
	copyBtn.style.top = '6px';
	copyBtn.style.right = '6px';
	copyBtn.style.padding = '2px 6px';
	copyBtn.style.fontSize = '12px';
	copyBtn.style.border = 'none';
	copyBtn.style.borderRadius = '3px';
	copyBtn.style.backgroundColor = 'rgba(255,255,255,0.2)';
	copyBtn.style.color = '#fff';
	copyBtn.style.cursor = 'pointer';
	copyBtn.style.display = 'none';

	copyBtn.onclick = function (e) {
		e.stopPropagation();
		navigator.clipboard.writeText(text).then(function () {
			copyBtn.textContent = 'Copied!';
			setTimeout(function () {
				copyBtn.textContent = 'Copy';
			}, 1000);
		}).catch(function (err) {
			console.error('Clipboard copy failed:', err);
		});
	};

	message.appendChild(copyBtn);

	var removeTimeout = null;

	message.onmouseenter = function () {
		clearTimeout(removeTimeout);
		copyBtn.style.display = 'block';
	};

	message.onmouseleave = function () {
		copyBtn.style.display = 'none';
		removeTimeout = setTimeout(removeMessage, 5000);
	};

	message.onclick = function () {
		removeMessage();
	};

	function removeMessage() {
		message.style.opacity = '0';
		setTimeout(function () {
			if (message.parentNode) {
				message.parentNode.removeChild(message);
			}
		}, 200);
	}

	container.appendChild(message);

	setTimeout(function () {
		message.style.opacity = '1';
	}, 10);

	removeTimeout = setTimeout(removeMessage, 5000);
}

// Öffentliche Funktionen
function success(msg) {
	showMessage('success', msg);
}

function error(msg) {
	showMessage('error', msg);
	console.error(msg);
}

function info(msg) {
	showMessage('info', msg);
	console.info(msg);
}

function warning(msg) {
	showMessage('warning', msg);
	console.warning(msg);
}


document.addEventListener("DOMContentLoaded", function () {
	var emojis = ["🏝️", "🍹", "⛱️", "🌞", "🌊", "🥥", "😎", "🏊", "🍉", "🏄", "🦜"];
	var randomEmoji = emojis[Math.floor(Math.random() * emojis.length)];
	var emojiContainer = document.getElementById("emoji");
	if (emojiContainer) {
		emojiContainer.textContent = randomEmoji;
	}
});


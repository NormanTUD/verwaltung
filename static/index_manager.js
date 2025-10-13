"use strict";

document.getElementById('indexForm').addEventListener('submit', async function(e){
	e.preventDefault();
	const checkboxes = Array.from(document.querySelectorAll('input[name="index"]:checked'));
	const indices = checkboxes.map(cb => {
		const [label, prop] = cb.value.split('|');
		return {label, property: prop};
	});
	if(indices.length === 0) {
		alert("No indices selected!");
		return;
	}
	const res = await fetch("/create_indices", {
		method: "POST",
		headers: {"Content-Type": "application/json"},
		body: JSON.stringify({indices})
	});
	const data = await res.json();
	if(data.status === "success") {
		alert("Indices created:\n" + data.created.map(i=>i.label+"."+i.property).join("\n"));
		window.location.reload();
	} else {
		alert("Error: " + data.message);
	}
});

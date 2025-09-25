"use strict";

function insertDefaultData(preset) {
	const textarea = document.getElementById('data');
	let data = '';

	if (preset === 'person_location') {
		data = `vorname,nachname,straße,stadt,plz
Maria,Müller,Hauptstraße 1,Berlin,10115
Hans,Schmidt,Marktplatz 5,Hamburg,20095
Anna,Fischer,Bahnhofsallee 12,München,80331`;
	} else if (preset === 'books_authors') {
		data = `buchtitel,erscheinungsjahr,vorname,nachname
The Cypher Key,2023,Maria,Müller
The Graph Odyssey,2022,Bob,Johnson
Neo's Journey,2024,Charlie,Brown`;
	}

	textarea.value = data.trim();
}

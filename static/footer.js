var backlink = document.getElementById('backLink');

if(backlink) {
	backlink.addEventListener('click', function(event) {
	    event.preventDefault(); // href erstmal verhindern

	    if (window.history.length > 1) {
		history.back();
	    } else {
		// Keine History, dann href öffnen
		window.location.href = this.href;
	    }
	});
}

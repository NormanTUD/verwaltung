document.addEventListener('DOMContentLoaded', () => {
	const width = window.innerWidth;
	const height = window.innerHeight;

	const svg = d3.select("body").append("svg")
		.attr("width", width)
		.attr("height", height);

	const container = svg.append("g");

	// Define arrowheads for directed links
	svg.append("defs").selectAll("marker")
		.data(["end"])
		.enter().append("marker")
		.attr("id", d => d)
		.attr("viewBox", "0 -5 10 10")
		.attr("refX", 15)
		.attr("refY", 0)
		.attr("markerWidth", 6)
		.attr("markerHeight", 6)
		.attr("orient", "auto")
		.append("path")
		.attr("d", "M0,-5L10,0L0,5")
		.attr("fill", "#999");

	const simulation = d3.forceSimulation()
		.force("link", d3.forceLink().id(d => d.id).distance(100))
		.force("charge", d3.forceManyBody().strength(-300))
		.force("center", d3.forceCenter(width / 2, height / 2));

	// Add zoom functionality
	const zoom = d3.zoom()
		.scaleExtent([0.1, 4])
		.on("zoom", (event) => {
			container.attr("transform", event.transform);
		});

	svg.call(zoom);

	fetch('/api/graph-data')
		.then(response => response.json())
		.then(graph => {
			if (graph.error) {
				console.error(graph.error);
				return;
			}

			const nodes = graph.nodes || [];
			const links = graph.links || [];

			// Prüfen ob überhaupt Daten da sind
			if (nodes.length === 0 && links.length === 0) {
				svg.append("text")
					.attr("x", width / 2)
					.attr("y", height / 2)
					.attr("text-anchor", "middle")
					.attr("font-size", "24px")
					.attr("fill", "gray")
					.text("Keine Graph-Daten vorhanden.");
				return;
			}

			// Group for links and their labels
			const linkGroup = container.append("g").attr("class", "links");

			const link = linkGroup.selectAll("line")
				.data(links)
				.enter().append("line")
				.attr("class", "link")
				.attr("marker-end", "url(#end)");

			const linkLabel = linkGroup.selectAll("text")
				.data(links)
				.enter().append("text")
				.attr("class", "link-text")
				.text(d => d.type);

			// Group for nodes and their labels
			const nodeGroup = container.append("g").attr("class", "nodes");

			const node = nodeGroup.selectAll("g.node")
				.data(nodes)
				.enter().append("g")
				.attr("class", "node")
				.call(d3.drag()
					.on("start", dragstarted)
					.on("drag", dragged)
					.on("end", dragended));

			node.append("circle")
				.attr("r", 10)
				.attr("fill", d => d.label === 'Movie' ? 'red' : 'blue');

			node.append("text")
				.attr("dy", "1.5em")
				.attr("text-anchor", "middle")
				.text(d => d.label)
				.append("tspan")
				.attr("x", 0)
				.attr("dy", "1.2em")
				.text(d => JSON.stringify(d.properties, null, 2));

			node.append("title")
				.text(d => JSON.stringify(d.properties, null, 2));

			simulation.nodes(nodes).on("tick", ticked);
			simulation.force("link").links(links);

			function ticked() {
				link
					.attr("x1", d => d.source.x)
					.attr("y1", d => d.source.y)
					.attr("x2", d => d.target.x)
					.attr("y2", d => d.target.y);

				linkLabel
					.attr("x", d => (d.source.x + d.target.x) / 2)
					.attr("y", d => (d.source.y + d.target.y) / 2);

				node.attr("transform", d => `translate(${d.x},${d.y})`);
			}
		})
		.catch(error => console.error('Error fetching graph data:', error));

	function dragstarted(event, d) {
		if (!event.active) simulation.alphaTarget(0.3).restart();
		d.fx = d.x;
		d.fy = d.y;
	}

	function dragged(event, d) {
		d.fx = event.x;
		d.fy = event.y;
	}

	function dragended(event, d) {
		if (!event.active) simulation.alphaTarget(0);
		d.fx = null;
		d.fy = null;
	}
});

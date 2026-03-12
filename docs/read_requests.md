# Reading from the DB
Read Requests are done via `api.read_as_table` submodule, which provides a blueprint that is registered in `api.__init__`.
At registration time we can inject the components for the [[#Parser]] and the [[#Responder]].

We get a driver from the `neo4j` python module, which needs to be registered at the `current_app.config["driver"]`.

## Anatomy of  Call
First the [[#Parser]] will evaluate the parameters that we reveive from the frontend and construct a [[ReadRequest]] from it.

Then the [[#Neo4jDB Object]] will read evaluate the parameters, construct the cypher and execute it.

Lastly, the [[#Responder]] will create a json response which we then return to the frontend.


## Parser
Evaluates the Parameters from the Frontend into a [[ReadRequest]].
## Neo4jDB - Interface
location: `api.neo4j_interface.Neo4jDB`
- built upon a Interface, so we may change to another implementation as needed.
	- shall implement CRUD oprations (only read as of now.)

### .read_data()
- consumes a [[ReadRequest]]
- creates a cypher query via `api.neo4j_interface.construct_cypher_query`
- executes it with a driver session.

## Responder

### [[Topology Based Responder]]
location: `api.read_as_table.strategies.py`

*This Component is used in ReadRequests, after the Neo4j Records have been retrieved this translates them into a table-based .json response.

It consists of 2 sub-components, the [[#Evaluator]] and the [[#Table Builder]]. And is an effort to make the translation of graph-based data to a table as solveable as possible.
### Evaluator
- Abstracts a Tree-based Topology of the Data with `NodeRoles`: `LEAF`, `ROOT`, `FORK`, `CHAIN`
- Found Loops (i.e. Node-Types that repeat in the tree) will have their information attached in the optional `TopologyTree.cycle_type`
#### Steps of the Evaluator
1. call the [[Topology Translator]]
2. extract `ordered_labels` from the tree(s)
3. build `props_by_type` out of the data (expansive)

### Table Builder
- Creates a `row` for every `record`
	- `cells` are initialized as the empty cell for every found `column`
- Iterates over `elements` of the `record`
	- `nodes` have their props injected into the cells.
	- if a `node-type` is seen more then once in a `record`, it gets moved to the right in the columns.
	- relationships get added
- Sorts the `rows` to group information of `single-nodes` together



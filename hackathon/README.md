# Hackathon

## Important Dates
* **Development Phase**: 22nd November 2024 (4 hours, in person)
* **Conclusions**: 29th November 2024 (2 hours), with each group presenting for 10-15 minutes

## Data Sources
* **Chicago Data Portal** - [Chicago Data Portal](https://data.cityofchicago.org/) (structured data)
    * See below some possible ways to extend the KG on public procurement data.
* **Project Gutenberg** - [Project Gutenberg](https://www.gutenberg.org/) (unstructured data)
    * [Alice's Adventures in Wonderland](https://www.gutenberg.org/cache/epub/11/pg11-images.html)
* **Open Graph Benchmark** - [Open Graph Benchmark](https://ogb.stanford.edu/) (graph data)
    * [MAG240M](https://ogb.stanford.edu/docs/lsc/mag240m/) - Heterogeneous Academic Graph ([textual information](http://snap.stanford.edu/ogb/data/lsc/mapping/mag240m_mapping.zip))
    * [WikiKG90Mv2](https://ogb.stanford.edu/docs/lsc/wikikg90mv2/) - Wiki Knowledge Graph ([textual information](http://snap.stanford.edu/ogb/data/lsc/mapping/wikikg90mv2_mapping.zip))
* Or you can leverage the datasets we explored during the course!

## Tasks
Here, we present a list of potential tasks to tackle during the hackathon.

### Building Tasks
Using the datasets provided in the "Data Sources" section, create a new KG. In this context, one of the key aspects is creating a graph data model that is suitable to model the data.

- **Structured data**: Directly read the files and ingest the information into Neo4j.
- **Unstructured data**: Use Large Language Models (LLMs) to extract key entities and relationships either in an open format or by defining a schema.
  
After the KG generation, you may define basic Cypher queries to explore the datasets.

### Enrichment Tasks
Building upon datasets discussed in lectures, consider ways to enrich the data with additional information. Here are a few examples:

#### Extend HPO with Italian Labels and Descriptions
Use the [Wikidata SPARQL Endpoint](https://query.wikidata.org/) to obtain labels and descriptions in Italian for HPO items:

```sparql
SELECT ?item ?itemLabel ?itemDescription ?hpoID WHERE {
  ?item wdt:P3841 ?hpoID.
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
}
LIMIT 10
```

#### Integrate Multiple Information into Chicago Data
* Integrating payment details - [Payments](https://data.cityofchicago.org/Administration-Finance/Payments/s4vu-giwb/about_data)
* Integrating department employees - [Current Employee Names, Salaries, and Position Titles](https://data.cityofchicago.org/Administration-Finance/Current-Employee-Names-Salaries-and-Position-Title/xzkq-xp2w/about_data)

#### Add Wikipedia Information to RAC Researchers
* Some researchers in the RAC datasets have corresponding Wikipedia pages (e.g., [Enrico Fermi](https://en.wikipedia.org/wiki/Enrico_Fermi)). Use LLMs to identify potential Wikipedia pages for researchers and add the related data to the KG. And then, you can use the Media Wiki API for retrieving the related Wikidata information: https://www.wikidata.org/w/api.php?action=wbgetentities&sites=enwiki&titles=Enrico_Fermi (use the title of the Wikipedia page).

### Analysis Tasks
Extend the analysis we performed on the datasets explored during the course.

#### Expand Reasoning Results in HPO
We previously explored methods to identify diseases related to subclasses of specific phenotypic abnormalities ([third query](https://github.com/giuseppefutia/bitbang-kg/tree/master/md02#analysis)). However, the query does not provide details about the specific sublass. Try this alternative procedure: [Neo4j Expand Paths Config](https://neo4j.com/labs/apoc/4.3/graph-querying/expand-paths-config/).

#### Add Updating Scenario in Chicago Dataset
In the context of Change Data Capture (CDC), we explored [how to add new nodes](https://github.com/giuseppefutia/bitbang-kg/blob/master/md03/importer/batch_new_node_simulation.py) and applying similarity algorithms and community detection to the affected subgraph. You can explore techniques for managing updates using the same approach.

#### Perform Centrality Analysis in RAG Data and Use LLM for Interpretation
Each centrality measure indicates a [node's importance within the network](https://github.com/giuseppefutia/bitbang-kg/blob/master/md04/importer/import_rac_gds.py). Add additional centrality measures to the RAC KG data using Neo4j, and make RAG/GraphRAG queries to interpret researchers' roles based on these centrality values.

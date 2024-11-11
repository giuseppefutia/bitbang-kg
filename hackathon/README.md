# Hackathon

## Important Dates
* **Development Phase**: 22nd November 2024 (4 hours, in person)
* **Conclusions**: 29th November 2024 (2 hours), with each group presenting for 10-15 minutes

## Data Sources
* **Chicago Data Portal** - [Chicago Data Portal](https://data.cityofchicago.org/) (structured data)
* **Project Gutenberg** - [Project Gutenberg](https://www.gutenberg.org/) (unstructured data)
* **Open Graph Benchmark** - [Open Graph Benchmark](https://ogb.stanford.edu/) (graph data)

## Building Knowledge Graphs from Scratch
Using the datasets provided in the "Data Sources" section, create a Knowledge Graph (KG). Define a graph data model specific to each data source. 

- **Structured data**: Directly read the files and ingest the information into Neo4j.
- **Unstructured data**: Use Large Language Models (LLMs) to extract key entities and relationships either in an open format or by defining a schema.
  
You may define basic Cypher queries to explore the datasets.

## Enrichment Tasks
Building upon datasets discussed in lectures, consider ways to enrich the data with additional information. Here are a few examples:

### Extend HPO with Italian Labels and Descriptions
Utilize the Wikidata SPARQL API to obtain labels and descriptions in Italian for HPO items:

```sparql
SELECT ?item ?itemLabel ?itemDescription ?hpoID WHERE {
  ?item wdt:P3841 ?hpoID.
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
}
LIMIT 10
```

### Integrate Payment Information into Chicago Data
Enhance the public contracts monitoring data by integrating payment details from the following dataset: [Chicago Public Contracts Data](https://dev.socrata.com/foundry/data.cityofchicago.org/s4vu-giwb).

### Add Wikipedia Information to RAC Researchers
Some researchers in the RAC datasets have corresponding Wikipedia pages (e.g., [Enrico Fermi](https://it.wikipedia.org/wiki/Enrico_Fermi)). Use LLMs to identify potential Wikipedia pages for researchers and add this data to the KG.

## Analysis Tasks

### Expand Reasoning Results in HPO
We previously explored methods to identify diseases related to subclasses of specific phenotypic abnormalities ([example query](https://github.com/giuseppefutia/bitbang-kg/tree/master/md02#analysis), third query). However, the query currently doesn’t specify subclasses. Try this alternative procedure: [Neo4j Expand Paths Config](https://neo4j.com/labs/apoc/4.3/graph-querying/expand-paths-config/).

### Add Updating Scenario in Chicago Dataset
In Change Data Capture, we examined adding new nodes and applying community detection to affected graph areas. Explore techniques for managing updates within the same framework.

### Perform Centrality Analysis in RAG Data and Use LLM for Interpretation
Each centrality measure indicates a node's importance within the network. Add additional centrality measures to the RAG data using Neo4j, and make RAG/GraphRAG queries to interpret researchers' roles based on these centrality values.

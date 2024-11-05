# Module 2
* [\[Google Slide Presentations\]](https://docs.google.com/presentation/d/1r08E8qqqLyIF61M6bpBZ3zR8341b5-DzeAIBOBPz9-I/edit?usp=sharing) - Course Presentation and Introduction to Graphs

## Create your Neo4j Doctor database
The goal of this module is to create a database to support medical practictioners in helping to detect rare diseases based on a collection of symptomps.

### Getting started
The code provided in this module requires that you have installed the Neosemantics plugin in your Neo4j instance.

### Install requirements
This module's `Makefile` assumes you have a virtual environemnt folder called `venv` 
at the reopository root folder. You can edit the`Makefile` and redefine the `PIP` variable
at the first line to match your configuration.
```shell
make init
```

### Download and import the datasets
Run this command to execute the importer code for each dataset.
```shell
make import
```

## Analysis
Getting all the phenotypic features associated with Type 1 diabetes:
```cypher
MATCH path=(dis:Disease)-[:HAS_PHENOTYPIC_FEATURE]->(phe:Hpo)
WHERE dis.id = "OMIM:222100"
RETURN path
```

Getting all the diseases with an association to defined phenotypic features:
```cypher
MATCH (phe:Hpo)
WHERE phe.label = "Growth delay"
OR phe.label = "Large knee"
OR phe.label = "Sensorineural hearing impairment"
OR phe.label = "Pruritus"
OR phe.label = "Type I diabetes mellitus"
WITH phe
MATCH path=(dis:Disease)-[:HAS_PHENOTYPIC_FEATURE]->(phe)
UNWIND dis as nodes
RETURN dis.id as disease_id, 
dis.label as disease_name,
collect(phe.label) as features,
count(nodes) as num_of_features
ORDER BY num_of_features DESC, disease_name
LIMIT 5
```

Getting all diseases related to the "Abnormality of the endocrine system":
```cypher
MATCH (cat:Hpo {label: "Abnormality of the endocrine system"})<-[:HAS_PHENOTYPIC_FEATURE]-(d:Disease)
RETURN d
```

Getting all diseases related to the "Abnormality of the endocrine system" (with reasoning):
```
MATCH (cat:Hpo {label: "Abnormality of the endocrine system"})
CALL n10s.inference.nodesInCategory(cat, { 
    inCatRel: "HAS_PHENOTYPIC_FEATURE", subCatRel: "SUBCLASSOF"})
YIELD node as dis
MATCH (dis)-[:HAS_PHENOTYPIC_FEATURE]->(phe:Hpo)
RETURN dis.label as disease, collect(DISTINCT phe.label) as features
ORDER BY size(features) ASC, disease
SKIP 200
LIMIT 5
```

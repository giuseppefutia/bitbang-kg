# Module 3
* [Building KGs from Structured Data](https://docs.google.com/presentation/d/1lKcwYW3Z4NkoOhrzhqJFjHxPkweEDlknqvb8VE9Cqis/edit?usp=sharing)

## Create your public contracts monitoring system
The goal of this module is to create a database to support the monitoring of public contracts released by the City of Chicago.

### Download source data (or db dump)
You can download the input files from the following [folder](https://drive.google.com/drive/folders/1I6JhP4mXR3V9vo8BifcdLqKyTHGDtKFn). For directly loading the db dump, you can download the dump and load it into the db folder created by Neo4j Desktop.

### Install requirements
This module's `Makefile` assumes you have a virtual environemnt folder called `venv` 
at the reopository root folder. You can edit the`Makefile` and redefine the `PIP` variable
at the first line to match your configuration.
```shell
make init
```

### Import the datasets
For the data ingestion phase, you have to update the memory heap in the Neo4j settings: `dbms.memory.heap.max_size=4G` (useful to running clustering/community detection algorithms).

Run this command to execute the importer code for each dataset.
```shell
make import
```

### Test Change Data Capture (CDC)
For testing Change Data Capture (CDC), you can run the following command:
```shell
make cdc
```

You can make small updates on your graph, like `MERGE (p:NodeTest) RETURN p` and see response of the CDC service in your terminal.

### Test batch import
You will find a basic example of a new record to ingest in the file `dataset/chicago/Owners_batch.csv`. You can run the following command to test the batch importing. The script will perform similarity computation and run the clustering/community algorithms on the subgraph affected by the import.
```shell
make simulate
```

### Restore db status
To restore the database to its state before the batch import, you can run the following command:
```shell
make clean
```

## Analysis
Which Departments collaborate with the Person mentioned in Record 3218056?
```cypher
MATCH path1=(r:PersonRecord)-[:RECORD_RESOLVED_TO]->(p:Person)-[:BELONGS_TO_ORG]->(m:Organization)
WHERE m.source = "LICENSES"  AND r.pk = 3218056 //Shawn Podgurski
OPTIONAL MATCH path2=(m)-[:IS_SIMILAR_TO]->(n:Organization)<-[:HAS_VENDOR]-()-[:INCLUDED_IN_CONTRACT]->()<-[:ASSIGNS_CONTRACT]-(:Department)
WHERE n.source = "CONTRACTS"
RETURN path1, path2
```

Are there businesses with expired licenses still executing city contracts?
```cypher
MATCH p=(l:LicenseRecord)-[:ORG_HAS_LICENSE]-(n:Organization)-[r:IS_SIMILAR_TO]-(m:Organization)<-[:HAS_VENDOR]-(c:ContractRecord) 
WHERE m.source = "CONTRACTS" AND n.source = "LICENSES" AND
      c.startDate IS NOT NULL AND
      l.endDate IS NOT NULL
// Change date format
WITH n, m, 
     apoc.date.parse(c.startDate, "ms", "MM/dd/yyyy") AS startDate,
     apoc.date.parse(l.endDate, "ms", "MM/dd/yyyy") as endDate
// Process date string as dates
WITH n, m,
     date(datetime({epochmillis: startDate})) AS startDate,
     date(datetime({epochmillis: endDate})) AS endDate
WITH n, m, min(startDate) as ContractDate, max(endDate) as LicenseDate
WHERE ContractDate > LicenseDate
RETURN n.name as OrganizationInLicenses, m.name as OrganizationInContracts, ContractDate, LicenseDate
```
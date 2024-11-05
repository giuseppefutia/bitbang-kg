# Module 3
* [\[Google Slide Presentations\]](https://docs.google.com/presentation/d/1JhF5BLIviPmdjDAVxqDkm8aMFkoesHb_Us5GP-T4POk/edit?usp=sharing) - Neo4j Use Cases on Structured Data.

## Create your public contracts monitoring system
The goal of this module is to create a database to support the monitoring of public contracts released by the City of Chicago.

### Download source data (or db dump)
You can download the input files from the following [`https://drive.google.com/drive/folders/1I6JhP4mXR3V9vo8BifcdLqKyTHGDtKFn`][folder]. For directly loading the db dump, you can download the dump and load it into the db folder created by Neo4j Desktop.

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

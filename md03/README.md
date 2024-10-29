# Module 3
* [\[Google Slide Presentations\]](https://docs.google.com/presentation/d/1JhF5BLIviPmdjDAVxqDkm8aMFkoesHb_Us5GP-T4POk/edit?usp=sharing) - Neo4j Use Cases on Structured Data.

## Create your public contracts monitoring system
The goal of this module is to create a database to support the monitoring of public contracts released by the City of Chicago.

### Install requirements
This module's `Makefile` assumes you have a virtual environemnt folder called `venv` 
at the reopository root folder. You can edit the`Makefile` and redefine the `PIP` variable
at the first line to match your configuration.
```shell
make init
```

### Import the datasets
For the data ingestion phase, you have to update the memory heap in the Neo4j settings: `dbms.memory.heap.max_size=4G`.

Run this command to execute the importer code for each dataset.
```shell
make import
```
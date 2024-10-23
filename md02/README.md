# Module 2
* [\[Google Slide Presentations\]](https://docs.google.com/presentation/d/1r08E8qqqLyIF61M6bpBZ3zR8341b5-DzeAIBOBPz9-I/edit?usp=sharing) - Course Presentation and Introduction to Graphs

# Create your Neo4j Doctor database
The goal of this module is to create a database to support medical practictioners in helping to detect rare diseases based on a collection of symptomps.

### Getting started
The code provided in this module requires that you have installed the Neosemantics plugin in your Neo4j instance.

#### Install requirements
This module's `Makefile` assumes you have a virtual environemnt folder called `venv` 
at the reopository root folder. You can edit the`Makefile` and redefine the `PIP` variable
at the first line to match your configuration.
```shell
make init
```

#### Download and import the datasets
Run this command to execute the importer code for each dataset.
```shell
make import
```

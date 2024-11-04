# Module 4
TODO: Include Lecture slides.

The content of this module is inspired by the book repository: https://github.com/alenegro81/knowledge-graph-applied-book/.

## Create a retrieval system for RAC documents
The goal of this module is to create KG from the Rockefeller Archive Center (RAC) documents and an agent that is able to extract information from its contents.

### Install requirements
This module's `Makefile` assumes you have a virtual environemnt folder called `venv` 
at the reopository root folder. You can edit the`Makefile` and redefine the `PIP` variable
at the first line to match your configuration.
```shell
make init
```

### Import the datasets
The ingestion phase leverages OpenAI APIs requires to specify an `apikey` in the `config.ini` file

Run this command to execute the importer code for each dataset.
```shell
make import
```

### Run the agent
You an ask a question the RAC KG by running the agent

```shell
make agent question="Who are the top three influencers of cyclotron research? And what is known about them?"
```
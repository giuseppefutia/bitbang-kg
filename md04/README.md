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

### Run the agents
You an ask a question the RAC KG by running different types of agent. These agents can be run with a default question (`How is Horovitz related to cyclotron?`) or you can specify your question in the command line.

The first type of agent is fully-based on the intercation with ChatGPT API. To execute this agent, you can run the following command:
```shell
make agent-llm question="Who are the top three influencers of cyclotron research according to the Rockefeller Archive Center? And what is known about them?"
```

The second type of agent leverages traditional Retrieval Augmented Generation (RAG) principles:

```shell
make agent-rag question="Who are the top three influencers of cyclotron research? And what is known about them?"
```

The third type of agent combines RAG and GraphRAG capabilities:
```shell
make agent-graph-rag question="Who are the top three influencers of cyclotron research? And what is known about them?"
```
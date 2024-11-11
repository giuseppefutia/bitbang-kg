import os
import logging
import configparser
from typing import List
from langchain_openai import OpenAIEmbeddings
from langchain_community.graphs import Neo4jGraph
from langchain_community.vectorstores import Neo4jVector

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)

class LangchainStore:
    def __init__(self, config_path: str = "config.ini"):
        # Load configuration
        config = configparser.ConfigParser()
        config.read(config_path)

        # Set Neo4j parameters
        self.neo4j_uri = config["neo4j"]["uri"]
        self.neo4j_user = config["neo4j"]["user"]
        self.neo4j_password = config["neo4j"]["password"]
        self.neo4j_db = config["neo4j"]["database"]

        # Initialize OpenAI configuration
        os.environ["OPENAI_API_KEY"] = config['openai']['apikey']

    
    def init_graph_store(self):
        """Initialize the Neo4j graph for general-purpose Cypher queries."""
        graph = Neo4jGraph(
            url=self.neo4j_uri,
            username=self.neo4j_user,
            password=self.neo4j_password,
            database=self.neo4j_db
        )
        return graph

    def init_vector_store(self, index_name: str = "embeddings", node_label: str = "Page",
                          text_node_properties: List[str] = ["text"], embedding_node_property: str = "embedding"):
        # Initialize Neo4j Vector store
        vector_index = Neo4jVector.from_existing_graph(
            OpenAIEmbeddings(),
            url=self.neo4j_uri,
            username=self.neo4j_user,
            password=self.neo4j_password,
            database=self.neo4j_db,
            index_name=index_name,
            node_label=node_label,
            text_node_properties=text_node_properties,
            embedding_node_property=embedding_node_property
        )

        return vector_index

if __name__ == "__main__":
    store = LangchainStore()
    store.init_graph_store()
    logging.info("Neo4j graph store initialized.")
    store.init_vector_store()
    logging.info("Neo4j vector store initialized.")

import os
import argparse

from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain.tools import StructuredTool
from langchain.agents import create_structured_chat_agent, AgentExecutor

from langchain_store import LangchainStore
from definitions import KG_SCHEMA
from tools import vector_search, kg_reader, kg_doc_selector, VectorSearchInput, KGReaderInput, REDiarySelectorInput

class Agent:
    def __init__(self):

        # Init LangchainStore for OpenAI API key setting
        config_path = os.path.join(os.path.dirname(__file__), '../../', 'config.ini')
        LangchainStore(config_path)

        # Set GPT model
        self.model = "gpt-4o-mini"

        # Init tools
        self.tools = self.init_tools()

        # Init agent
        self.agent = self.init_agent()
    
    def init_tools(self):
        return [
        StructuredTool.from_function(
            func=vector_search,
            name="Diaries-vector-search",
            args_schema=VectorSearchInput,
            return_direct=False,
            description="""This is a backup tool based on vector search to be used ONLY when no other tool provides nor  
            is capable to provide question-relevant context.
            Try the other tools first! Then use this one as a last resort.
            Use it when you need to answer questions about details within Warren Weaver's diaries that are too
            fine-grained to be modeled in the Knowledge Graph.
            When the other tools return nothing useful, execute this tool before generating final answer.
            Always use full question as input, without any changes!""",
        ),
        StructuredTool.from_function(
            func=kg_reader,
            name="KnowledgeGraph-reader",
            args_schema=KGReaderInput,
            description=f"""Useful when you need to answer questions for which the information stored in the KG
            is sufficient, for example about relationships among entities such as people, organizations and occupations.
            Also useful for any sort of aggregation like counting the number of people per occupation etc.
            This tool translates the question into a Cypher query, executes it and returns results.

            Full Knowledge Graph schema in Cypher syntax to help you decide whether this tool can be used or not:
            {KG_SCHEMA}

            Always use full question as input, without any changes!""",
        ),
        StructuredTool.from_function(
            func=kg_doc_selector,
            name="KG-based-document-selector",
            args_schema=REDiarySelectorInput,
            return_direct=False,
            description=(
                "Use this as a default tool for document (diary entries) retrieval when the question asks for detailed "
                "information regarding interaction between different entities. "
                "Run a query for each action you have identified. "
                "The entities and relationship between them must be modeled within the KG (see schema below), but the KG itself "
                "does not contain enough details to provide the answer (in which case you should use the "
                "KnowledgeGraph-reader tool.\n\n"
                "Full Knowledge Graph schema in Cypher syntax to help you decide whether this tool can be used or not:\n"
                f"{KG_SCHEMA}"
            )
        )]
    
    def init_agent(self, agent_prompt="./rag/prompt_structured.txt"):
        llm = ChatOpenAI(model=self.model, temperature=0)
        with open(agent_prompt, 'r') as f:
            system = f.read()
            human = ("{input}\n\n"
                    "{agent_scratchpad}\n\n"
                    "(reminder to respond in a JSON blob no matter what)")
            prompt = ChatPromptTemplate.from_messages(
                [
                    ("system", system),
                    MessagesPlaceholder("chat_history", optional=True),
                    ("human", human),
                ]
            )
        return create_structured_chat_agent(llm, self.tools, prompt)
    
    def run(self, question: str):
        agent_executor = AgentExecutor(agent=self.agent, 
                                       tools=self.tools, 
                                       max_iterations=5, 
                                       return_intermediate_steps=True, verbose=True)
        response = agent_executor.invoke({"input": question})
        print(f"\n### Agent's response: {response['output']}")
        
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the LangChain Agent with a specified question.")
    parser.add_argument(
        "question", 
        nargs="?", 
        help="The question you want the agent to answer."
    )
    args = parser.parse_args()
    question = args.question or "How is Horovitz related to cyclotron?"
    agent = Agent()
    answer = agent.run(question)

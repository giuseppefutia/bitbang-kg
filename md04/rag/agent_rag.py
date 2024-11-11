import os
import argparse
from openai import OpenAI

from langchain.chains import RetrievalQA
from langchain_openai import ChatOpenAI

from langchain_store import LangchainStore

class Agent:
    def __init__(self):

        # Init LangchainStore for OpenAI API key setting
        config_path = os.path.join(os.path.dirname(__file__), '../../', 'config.ini')
        store = LangchainStore(config_path)
        self.client = OpenAI(api_key=os.environ['OPENAI_API_KEY'])
        self.vector_index = store.init_vector_store()
        self.model = "gpt-4o-mini"
    
    def run(self, question: str):
        """
        response = self.vector_index.similarity_search_with_score(question, k=3)
        print(f"\n### Retrieved documents: ")
        for r in response:
            print(f"------\nScore: {r[1]}")
            print(r[0].page_content)
        """
      
        retriever = self.vector_index.as_retriever(k=3, score_threshold=0.7)
        docs = retriever.invoke(question)
        print(f"### Retrieved documents")
        for r in docs:
            print(r.page_content)
        
        
        vector_qa = RetrievalQA.from_chain_type(
            llm=ChatOpenAI(model_name=self.model, temperature=0.2),
            chain_type="stuff",
            retriever=self.vector_index.as_retriever(k=3, score_threshold=0.7)
        )
        response = vector_qa.invoke(question)
        
        
        
        print(f"### Agent's response (RAG): {response['result']}")
        """
         response = self.vector_index.similarity_search_with_score(question, k=2)
        print(f"\n### Agent's response: ")
        for r in response:
            print(f"------\nScore: {r[1]}")
            print(r[0].page_content)
        """
       

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

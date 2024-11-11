import os
import argparse
from openai import OpenAI
from langchain_store import LangchainStore

class Agent:
    def __init__(self, model: str = "gpt-4o-mini", system: str = None):
        self.model = model
        self.system = system
        self.messages = list()
        
        # Init LangchainStore for OpenAI API key setting
        config_path = os.path.join(os.path.dirname(__file__), '../../', 'config.ini')
        LangchainStore(config_path)

        self.client = OpenAI(api_key=os.environ['OPENAI_API_KEY'])

        if self.system is None or len(self.system) == 0:
            self.system = "You are an AI assistant providing straightforward concise answers."
        self.messages.append({"role": "system", "content": self.system})

    def execute(self) -> str:
        completion = self.client.chat.completions.create(
                        model=self.model,
                        temperature=0,
                        messages=self.messages)
        return completion.choices[0].message.content
    
    def run(self, message: str) -> str:
        self.messages.append({"role": "user", "content": message})
        answer = self.execute()
        self.messages.append({"role": "assistant", "content": answer})
        print(f"\n### Agent's response: {answer}")


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

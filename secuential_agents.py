import asyncio
from typing import cast
from agent_framework import ChatMessage, Role, SequentialBuilder, WorkflowOutputEvent
from agent_framework.azure import AzureAIAgentClient
from azure.identity import AzureCliCredential
from agents.agents_functions.table_agent_functions import table_agent_functions
import yaml

with open("agents/instructions/instructions.yaml", "r", encoding="utf-8") as f:
    instructions = yaml.safe_load(f)

async def main():
    credential = AzureCliCredential()

    async with AzureAIAgentClient(async_credential=credential) as chat_client:
        # --- Table agent with its functions (tools) ---
        table_agent = chat_client.create_agent(
            name="table_agent",
            instructions=instructions["table_agent_instructions"],
            tools=table_agent_functions  
        )

        # # --- Main orchestrator agent ---
        # main_agent = chat_client.create_agent(
        #     name="main_agent",
        #     instructions=instructions["main_agent_instructions"]
        # )

        # --- Workflow ---
        # workflow = SequentialBuilder().participants([main_agent, table_agent]).build()
        workflow = SequentialBuilder().participants([table_agent]).build()

        print("Chat with the multi-agent system. Type 'quit' to exit.\n")
        while True:
            user_input = input("Enter a prompt: ")
            if user_input.lower() == "quit":
                break

            outputs: list[list[ChatMessage]] = []
            async for event in workflow.run_stream(user_input):
                if isinstance(event, WorkflowOutputEvent):
                    outputs.append(cast(list[ChatMessage], event.data))

            if outputs:
                seen_texts = set()
                for msg in outputs[-1]:  # only take the final workflow output
                    if msg.role == Role.ASSISTANT and msg.text not in seen_texts:
                        seen_texts.add(msg.text)
                        name = msg.author_name or "assistant"
                        print(f"{'-'*60}\n[{name}]\n{msg.text}")

if __name__ == "__main__":
    asyncio.run(main())


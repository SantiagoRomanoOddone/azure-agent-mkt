import asyncio
from typing import cast
from agent_framework import ChatMessage, Role, SequentialBuilder, WorkflowOutputEvent
from agent_framework.azure import AzureAIAgentClient
from azure.identity import AzureCliCredential
from agent_functions import agent_functions 

async def main():
    table_agent_instructions = """
    You handle queries on 'contractual' and 'earned' tables.
    Validate table and columns first, then generate and execute SQL query.
    """

    main_agent_instructions = """
    You are the orchestrator. Analyze user requests.
    If the request involves tables, delegate to the 'table_agent'.
    Return the table_agent response to the user.
    """

    credential = AzureCliCredential()

    async with AzureAIAgentClient(async_credential=credential) as chat_client:
        # --- Table agent with its functions (tools) ---
        table_agent = chat_client.create_agent(
            name="table_agent",
            instructions=table_agent_instructions,
            tools=agent_functions  
        )

        # --- Main orchestrator agent ---
        main_agent = chat_client.create_agent(
            name="main_agent",
            instructions=main_agent_instructions
        )

        # --- Workflow ---
        workflow = SequentialBuilder().participants([main_agent, table_agent]).build()

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
                for i, msg in enumerate(outputs[-1], start=1):
                    name = msg.author_name or ("assistant" if msg.role == Role.ASSISTANT else "user")
                    print(f"{'-'*60}\n{i:02d} [{name}]\n{msg.text}")

if __name__ == "__main__":
    asyncio.run(main())

import asyncio
from typing import cast
from agent_framework import ChatMessage, Role, SequentialBuilder, WorkflowOutputEvent
from agent_framework.azure import AzureAIAgentClient
from azure.identity import AzureCliCredential
from agent_functions.table_agent_functions import table_agent_functions

async def main():
    table_agent_instructions = """
    You are a data assistant for the tables 'contractual' and 'earned'.

    Process:
    1. Identify the exact table the user wants to query ('contractual' or 'earned').
    - If unclear, ask the user to specify the table before proceeding.
    2. Retrieve the table schema using `get_table_schema(table)` which returns each column’s name, data type, and description. Use the description to provide more context when generating answers or SQL.
    3. Validate that all columns referenced by the user exist in the schema.
    4. Generate and execute a SQL query **only** after validation.
    5. Return:
    - The answer in plain language.
    - The raw SQL query.

    If the table name is invalid or columns are missing, clearly explain what is valid instead of running a query.
    """


    main_agent_instructions = """
    You are the orchestrator agent.
    - Analyze the user request carefully.
    - If the request involves tables, make sure you know which table is requested.
    - Only delegate to 'table_agent' once the table is clearly identified.
    - Return the table_agent's response to the user.
    """

    credential = AzureCliCredential()

    async with AzureAIAgentClient(async_credential=credential) as chat_client:
        # --- Table agent with its functions (tools) ---
        table_agent = chat_client.create_agent(
            name="table_agent",
            instructions=table_agent_instructions,
            tools=table_agent_functions  
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
                seen_texts = set()
                for msg in outputs[-1]:  # only take the final workflow output
                    if msg.role == Role.ASSISTANT and msg.text not in seen_texts:
                        seen_texts.add(msg.text)
                        name = msg.author_name or "assistant"
                        print(f"{'-'*60}\n[{name}]\n{msg.text}")

if __name__ == "__main__":
    asyncio.run(main())


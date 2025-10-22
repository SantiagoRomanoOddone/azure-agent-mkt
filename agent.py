import asyncio
from typing import cast
from agent_framework import ChatMessage, Role, SequentialBuilder, WorkflowOutputEvent
from agent_framework.azure import AzureAIAgentClient
from azure.identity import AzureCliCredential

async def main():
    # Child agent instructions
    table_agent_instructions = """
    You handle queries on 'contractual' and 'earned' tables.
    First validate the table and columns using get_table_schema().
    Then generate and execute a SQL query and return the result.
    """

    # Main orchestrator instructions
    main_agent_instructions = """
    You are the orchestrator. Analyze the user request.
    If it involves tables, delegate to the 'table_agent'.
    Return the table_agent response to the user.
    """

    # Connect to Azure AI
    credential = AzureCliCredential()
    async with AzureAIAgentClient(async_credential=credential) as chat_client:

        # Create child table agent
        table_agent = chat_client.create_agent(
            name="table_agent",
            instructions=table_agent_instructions
        )

        # Create main orchestrator agent
        main_agent = chat_client.create_agent(
            name="main_agent",
            instructions=main_agent_instructions
        )

        # Build sequential workflow: main_agent first, table_agent second
        workflow = SequentialBuilder().participants([main_agent, table_agent]).build()

        # User prompt
        prompt = "Give me the total value of ME_value in the earned table"

        outputs: list[list[ChatMessage]] = []
        async for event in workflow.run_stream(prompt):
            if isinstance(event, WorkflowOutputEvent):
                outputs.append(cast(list[ChatMessage], event.data))

        # Print results
        if outputs:
            for i, msg in enumerate(outputs[-1], start=1):
                name = msg.author_name or ("assistant" if msg.role == Role.ASSISTANT else "user")
                print(f"{'-'*60}\n{i:02d} [{name}]\n{msg.text}")

if __name__ == "__main__":
    asyncio.run(main())

import asyncio
from typing import cast
from agent_framework import ChatAgent, MagenticBuilder, MagenticCallbackEvent, MagenticCallbackMode, MagenticFinalResultEvent, MagenticOrchestratorMessageEvent, MagenticAgentDeltaEvent
from agent_framework.azure import AzureAIAgentClient
from azure.identity import AzureCliCredential
from agent_functions import agent_functions

async def main():
    credential = AzureCliCredential()

    # --- Define agents ---
    table_agent = ChatAgent(
        name="table_agent",
        description="Data assistant for tables 'contractual' and 'earned'",
        instructions="""
            You are a data assistant for the tables 'contractual' and 'earned'. 
            Process:
            1. Identify the exact table the user wants to query.
            2. Retrieve schema via get_table_schema(table).
            3. Validate columns.
            4. Generate SQL after validation.
            5. Return plain answer + raw SQL. 
            Explain clearly if table/columns are invalid.
        """,
        chat_client=AzureAIAgentClient(async_credential=credential),
        tools=agent_functions
    )

    main_agent = ChatAgent(
        name="main_agent",
        description="Orchestrator agent",
        instructions="""
            You are the orchestrator agent.
            Only delegate to 'table_agent' once the table is clearly identified.
            Return the table_agent's response to the user.
        """,
        chat_client=AzureAIAgentClient(async_credential=credential)
    )

    # --- Callback for streaming ---
    last_stream_agent_id: str | None = None
    stream_line_open: bool = False

    async def on_event(event: MagenticCallbackEvent):
        nonlocal last_stream_agent_id, stream_line_open
        if isinstance(event, MagenticOrchestratorMessageEvent):
            print(f"\n[ORCH:{event.kind}]\n{getattr(event.message, 'text', '')}\n{'-'*26}")
        elif isinstance(event, MagenticAgentDeltaEvent):
            if last_stream_agent_id != event.agent_id or not stream_line_open:
                if stream_line_open: print()
                print(f"\n[STREAM:{event.agent_id}]: ", end="", flush=True)
                last_stream_agent_id = event.agent_id
                stream_line_open = True
            print(event.text, end="", flush=True)
        elif isinstance(event, MagenticFinalResultEvent):
            print("\n" + "="*50)
            print("FINAL RESULT:")
            if event.message is not None:
                print(event.message.text)
            print("="*50)

    # --- workflow ---
    workflow = (
    MagenticBuilder()
    .participants(main=main_agent, table=table_agent)
    .on_event(on_event, mode=MagenticCallbackMode.STREAMING)
    .with_standard_manager(
        chat_client=AzureAIAgentClient(async_credential=credential),
        max_round_count=5,
        max_stall_count=3,
        max_reset_count=2
    )
    .build()
    )

    print("Chat with the multi-agent system. Type 'quit' to exit.\n")
    while True:
        user_input = input("Enter a prompt: ")
        if user_input.lower() == "quit":
            break
        async for event in workflow.run_stream(user_input):
            pass  # TODO 

if __name__ == "__main__":
    asyncio.run(main())

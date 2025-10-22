import os
from dotenv import load_dotenv
from azure.identity import DefaultAzureCredential
from azure.ai.agents import AgentsClient
from azure.ai.agents.models import ConnectedAgentTool, MessageRole, ListSortOrder, ToolSet, FunctionTool
from agent_functions import agent_functions

os.system('cls' if os.name=='nt' else 'clear')
load_dotenv()

project_endpoint = os.getenv("PROJECT_ENDPOINT")
model_deployment = os.getenv("MODEL_DEPLOYMENT_NAME")

agents_client = AgentsClient(
    endpoint=project_endpoint,
    credential=DefaultAzureCredential(
        exclude_environment_credential=True,
        exclude_managed_identity_credential=True
    ),
)

with agents_client:
    # Table agent
    functions = FunctionTool(agent_functions)
    toolset = ToolSet()
    toolset.add(functions)
    agents_client.enable_auto_function_calls(toolset)

    table_agent = agents_client.create_agent(
        model=model_deployment,
        name="table_agent",
        instructions="You handle queries on 'contractual' and 'earned' tables using get_table_schema() first.",
        toolset=toolset
    )
    table_agent_tool = ConnectedAgentTool(
        id=table_agent.id,
        name="table_agent",
        description="Handles table queries and schema checks"
    )

    # Orchestrator agent
    orchestrator = agents_client.create_agent(
        model=model_deployment,
        name="orchestrator_agent",
        instructions="You analyze user requests and call the 'table_agent' if needed, then return its response.",
        tools=[table_agent_tool.definitions[0]]
    )

    # Thread
    thread = agents_client.threads.create()
    prompt = "give me the total value of the ME_value of the earned table"

    agents_client.messages.create(
        thread_id=thread.id,
        role=MessageRole.USER,
        content=prompt
    )

    run = agents_client.runs.create_and_process(
        thread_id=thread.id,
        agent_id=orchestrator.id
    )

    if run.status == "failed":
        print(f"Run failed: {run.last_error}")

    messages = agents_client.messages.list(thread_id=thread.id, order=ListSortOrder.ASCENDING)
    for message in messages:
        if message.text_messages:
            last_msg = message.text_messages[-1]
            print(f"{message.role}:\n{last_msg.text.value}\n")

    # Cleanup
    agents_client.delete_agent(orchestrator.id)
    agents_client.delete_agent(table_agent.id)

import os
from dotenv import load_dotenv
from typing import Any
from pathlib import Path


# Add references
from azure.identity import DefaultAzureCredential
from azure.ai.agents import AgentsClient
from azure.ai.agents.models import FunctionTool, ToolSet, ListSortOrder, MessageRole
from agent_functions import agent_functions

def main(): 

    # Clear the console
    os.system('cls' if os.name=='nt' else 'clear')

    # Load environment variables from .env file
    load_dotenv()
    project_endpoint= os.getenv("PROJECT_ENDPOINT")
    model_deployment = os.getenv("MODEL_DEPLOYMENT_NAME")


    # Connect to the Agent client
    agent_client = AgentsClient(
    endpoint=project_endpoint,
    credential=DefaultAzureCredential
        (exclude_environment_credential=True,
         exclude_managed_identity_credential=True)
    )


    # Define an agent that can use the custom functions
    with agent_client:
        # Adds your set of custom functions to a toolset
        functions = FunctionTool(agent_functions)
        toolset = ToolSet()
        toolset.add(functions)
        agent_client.enable_auto_function_calls(toolset)
        
        # Creates an agent that uses the toolset.
        agent = agent_client.create_agent(
            model=model_deployment,
            name="table-agent",
            instructions="""
                You are a data assistant. You have access to one or more tables with known schemas.
                When a user asks a question about the data, you must:
                1. Understand which table and columns the user is referring to.
                2. Generate a valid SQL query to answer the question.
                3. Execute the SQL query using the provided function and return the results.
                Always return the answer clearly, do not return raw SQL unless asked.
                Example:
                User: What are the unique values in column2?
                Agent: Executes SELECT DISTINCT column2 FROM table_name and returns the values.
            """,
            toolset=toolset
        )

        # Runs a thread with a prompt message from the user.
        thread = agent_client.threads.create()
        print(f"You're chatting with: {agent.name} ({agent.id})")

        # Loop until the user types 'quit'
        while True:
            # Get input text
            # user_prompt = input("Enter a prompt (or type 'quit' to exit): ")
            user_prompt = 'How many unique names do I have in the table1?'
            if user_prompt.lower() == "quit":
                break
            if len(user_prompt) == 0:
                print("Please enter a prompt.")
                continue

            # Send a prompt to the agent
            message = agent_client.messages.create(
            thread_id=thread.id,
            role="user",
            content=user_prompt
            )
            #Using the create_and_process method to run the thread enables the agent to automatically 
            #find your functions and choose to use them based on their names and parameters.
            run = agent_client.runs.create_and_process(thread_id=thread.id, agent_id=agent.id)

            # Checks the status of the run in case there’s a failure
            if run.status == "failed":
                print(f"Run failed: {run.last_error}")
                
            # Retrieves the messages from the completed thread and displays the last one sent by the agent.
            last_msg = agent_client.messages.get_last_message_text_by_role(
            thread_id=thread.id,
            role=MessageRole.AGENT,
            )
            if last_msg:
                print(f"Last Message: {last_msg.text.value}")

        # Displays the conversation history
        print("\nConversation Log:\n")
        messages = agent_client.messages.list(thread_id=thread.id, order=ListSortOrder.ASCENDING)
        for message in messages:
            if message.text_messages:
                last_msg = message.text_messages[-1]
                print(f"{message.role}: {last_msg.text.value}\n")

        # Deletes the agent and thread when they’re no longer required.
        agent_client.delete_agent(agent.id)
        print("Deleted agent")
    



if __name__ == '__main__': 
    main()
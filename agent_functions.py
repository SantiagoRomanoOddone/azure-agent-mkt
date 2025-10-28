from typing import Any, Callable, List
import pandas as pd
import sqlite3

column_descriptions = {
    "contractual": {
        "Incidence_Date": "Date of the record",
        "Refresh_Date": "Date of last update",
        "Incidence_Timezone": "Timezone of the incidence",
        "Partner_Organization": "Partner involved",
        "Channel": "Marketing or broadcast channel",
        "Asset_Type": "Type of content or asset",
        "Seasonal_Event_Name": "Name of seasonal event",
        "Social_Platform": "Social media platform",
        "Social_Account": "Social account name",
        "Account_Handle": "Handle of social account",
        "Unique_Social_ID": "Unique identifier for social content",
        "Digital_Source": "Digital content source",
        "Unique_Digital_ID": "Unique digital ID",
        "Media_Type": "Type of media",
        "URL": "Link to asset",
        "Market": "Target market",
        "Unique_Broadcast_ID": "Unique broadcast ID",
        "Broadcast_Network_(US)": "US broadcast network",
        "Broadcast_Network_(UK)": "UK broadcast network",
        "Broadcast_Network_(SP)": "SP broadcast network",
        "Total_Impressions": "Total audience reached",
        "Social_Impressions": "Social media impressions",
        "Digital_Impressions": "Digital impressions",
        "Broadcast_Impressions_(US|HHLD)": "US household broadcast impressions",
        "Broadcast_Impressions_(US|P2+)": "US adult P2+ broadcast impressions",
        "Broadcast_Impressions_(UK|P2+)": "UK P2+ broadcast impressions",
        "Broadcast_Impressions_(SP|P2+)": "SP P2+ broadcast impressions",
        "Attendance": "Attendance metric",
        "Visibility": "Visibility metric",
        "Duration": "Duration metric",
        "Link_Clicks": "Number of link clicks",
        "Engagements": "User engagements",
        "Video_Views": "Number of video views",
        "ME_Value": "Marketing efficiency value, also considered as MEV",
        "Organic_or_Paid": "Organic or paid content",
        "Paid_Budget": "Paid budget amount",
        "Home_Team": "Home team name",
        "Away_Team": "Away team name",
        "Brand_1": "Primary brand",
        "Brand_2": "Secondary brand",
        "Content_Message": "Content message",
        "Customer_Journey_Stage": "Stage in customer journey",
        "CSA_Targeted": "CSA targeting info",
        "Audience_Persona": "Audience persona"
    },
    "earned": {
        "Incdence_Date": "Date of the record",
        "Refresh_Date": "Date of last update",
        "Partner_Organization": "Partner involved",
        "Channel": "Marketing or broadcast channel",
        "Asset_Name": "Name of the asset",
        "Seasonal_Event_Name": "Name of seasonal event",
        "Social_Platform": "Social media platform",
        "Social_Account": "Social account name",
        "Account_Handle": "Handle of social account",
        "Unique_Social_ID": "Unique identifier for social content",
        "Digital_Source": "Digital content source",
        "Unique_Digital_ID": "Unique digital ID",
        "Media_Type": "Type of media",
        "URL": "Link to asset",
        "Market": "Target market",
        "Unique_Broadcast_ID": "Unique broadcast ID",
        "Broadcast_Network_(US)": "US broadcast network",
        "Broadcast_Network_(Local_H)": "US local H network",
        "Broadcast_Network_(Local _A)": "US local A network",
        "Broadcast_Network_(UK)": "UK broadcast network",
        "Broadcast_Network_(SP)": "SP broadcast network",
        "Total_Impressions": "Total audience reached",
        "Social_Impressions": "Social media impressions",
        "Digital_Impressions": "Digital impressions",
        "Broadcast_Impressions (US|HHLD)": "US household broadcast impressions",
        "Broadcast_Impressions_(US National|P2+)": "US adult P2+ national impressions",
        "Broadcast_Impressions_(US Local H|P2+)": "US local H P2+ impressions",
        "Broadcast_Impressions_(US Local A|P2+)": "US local A P2+ impressions",
        "Broadcast_Impressions_(UK|P2+)": "UK P2+ broadcast impressions",
        "Broadcast_Impressions_(SP|P2+)": "SP P2+ broadcast impressions",
        "Engagements": "User engagements",
        "Video_Views": "Number of video views",
        "Exposures": "Number of exposures",
        "Video_Length": "Video length",
        "Duration_per_Exposure": "Duration per exposure",
        "30_Sec_Equivalent": "30-second equivalent metric",
        "Duration_Factor": "Duration factor metric",
        "EXT_Factor": "EXT factor metric",
        "ME_Score": "Marketing efficiency score",
        "ME_Value": "Marketing efficiency value, also considered as MEV",
        "Home_Team": "Home team name",
        "Away_Team": "Away team name",
        "Brand_1": "Primary brand",
        "Brand_2": "Secondary brand",
        "Content_Message": "Content message",
        "Customer_Journey_Stage": "Stage in customer journey",
        "CSA_Targeted": "CSA targeting info",
        "Audience_Persona": "Audience persona"
    }
}

# Updated schema function
def get_table_schema(table: str) -> Any:
    """
    Returns the schema (column names, data types) and descriptions of the specified table.
    """
    if table == 'contractual':
        df = pd.read_csv("data/contractual_dummy_data.csv")
    elif table == 'earned':
        df = pd.read_csv("data/earned_dummy_data.csv")
    else:
        return f"Table '{table}' not found."

    schema = []
    for col in df.columns:
        desc = column_descriptions.get(table, {}).get(col, "No description available")
        schema.append({"column": col, "dtype": str(df[col].dtype), "description": desc})

    return schema



def execute_sql(query: str, table: str) -> Any:
    """
    Executes a SQL query on the specified table's CSV data.
    The table is loaded into an in-memory SQLite database for querying.
    """
    if table == "contractual":
        df = pd.read_csv("data/contractual_dummy_data.csv")
    elif table == "earned":
        df = pd.read_csv("data/earned_dummy_data.csv")
    else:
        return f"Table '{table}' not found."

    # Create in-memory SQLite DB and register it under the real table name
    conn = sqlite3.connect(":memory:")
    df.to_sql(table, conn, index=False, if_exists="replace")

    # Execute the query
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()

    conn.close()
    return results



agent_functions: List[Callable[..., Any]] = [execute_sql, get_table_schema]
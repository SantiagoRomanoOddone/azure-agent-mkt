from typing import Any, Callable, List
import pandas as pd
import sqlite3

column_descriptions = {
        "contractual": {
        "Incidence_Date": "What date did the content go live?",
        "Refresh_Date": "What date was this data entered into the sheet?",
        "Incidence_Timezone": "What timezone did the asset go live in?",
        "Partner_Organization": "What property did this content involve? (NFL, Premier League, Big 12, Alpine, La Liga, Special Olympics, BJK Cup, or WNBA)",
        "Channel": "What channel did this exposure come from? (Social, Digital, Broadcast, or In-Stadium)",
        "Asset_Type": "What type of asset was it? Example: Social = sponsored post, Digital = banner ads or pre-roll, Broadcast = 30s unit",
        "Seasonal_Event_Name": "During which part of the season did this asset go live? (Week number, tentpole event, or program — must match Earned dataset)",
        "Social_Platform": "What platform did the social exposure occur on? (Facebook, Instagram, Twitter, TikTok)",
        "Social_Account": "What partner posted the content? (Only for Social)",
        "Account_Handle": "The @ handle of the social account that posted the content",
        "Unique_Social_ID": "Unique social post identifier (concatenation of date, account, and URL)",
        "Digital_Source": "What website published the asset? (Only for Digital)",
        "Unique_Digital_ID": "Unique digital article identifier (concatenation of date, source, article title, and URL)",
        "Media_Type": "Was this photo or video content? (Broadcast = video, Social/Digital can vary)",
        "URL": "Link to the social post or digital asset",
        "Market": "Was this regional or national? (Regional = not broadcasted nationally, National = broadcasted nationally)",
        "Unique_Broadcast_ID": "Unique broadcast program identifier (concatenation of date, broadcast, seasonal event, and home team if applicable)",
        "Broadcast_Network_(US)": "What US TV broadcaster aired the asset? (Only for US Broadcast)",
        "Broadcast_Network_(UK)": "What UK TV broadcaster aired the asset? (Only for UK Broadcast)",
        "Broadcast_Network_(SP)": "What Spanish TV broadcaster aired the asset? (Only for Spain Broadcast)",
        "Total_Impressions": "How many total impressions did the asset generate across the relevant channel?",
        "Social_Impressions": "How many total impressions did the social content garner? (Not applicable for TikTok or YouTube — those use video views)",
        "Digital_Impressions": "How many total impressions did the digital asset garner? (Only for Digital)",
        "Broadcast_Impressions_(US|HHLD)": "How many household impressions did the US broadcast asset garner? (Only for US Broadcast)",
        "Broadcast_Impressions_(US|P2+)": "How many total impressions did the US broadcast asset garner? (Only for US Broadcast)",
        "Broadcast_Impressions_(UK|P2+)": "How many total impressions did the UK broadcast asset garner? (Only for UK Broadcast)",
        "Broadcast_Impressions_(SP|P2+)": "How many total impressions did the Spain broadcast asset garner? (Only for Spain Broadcast)",
        "Attendance": "How many people attended the event?",
        "Visibility": "How visible was the signage?",
        "Duration": "How many seconds did the signage run for? (Rotational signage only)",
        "Link_Clicks": "How many times was the ad clicked?",
        "Engagements": "How many times was the sponsored social post engaged with?",
        "Video_Views": "How many video views did the sponsored social post garner? (Not applicable if photo)",
        "ME_Value": "Also considered as MEV, Media equivalency value based on ad rates, duration, and Microsoft integration level",
        "Organic_or_Paid": "Was the content posted organically or paid/boosted?",
        "Paid_Budget": "How much money was spent boosting the content/ad?",
        "Home_Team": "Home team name (Only for broadcasted games)",
        "Away_Team": "Away team name (Only for broadcasted games)",
        "Brand_1": "Primary Microsoft brand focus (Copilot, Surface, Windows, Azure, Teams)",
        "Brand_2": "Secondary Microsoft brand focus, if applicable (Copilot, Surface, Windows, Azure, Teams)",
        "Content_Message": "Post copy (for social) or article title (for digital)",
        "Customer_Journey_Stage": "To be populated by Microsoft",
        "CSA_Targeted": "To be populated by Microsoft",
        "Audience_Persona": "To be populated by Microsoft"
    },
    "earned": {
        "Incidence_Date": "What date did this exposure happen on?",
        "Refresh_Date": "What date was this data entered into the sheet?",
        "Partner_Organization": "What property did this exposure come from? (NFL, Premier League, Big 12, Alpine, La Liga, Special Olympics, BJK Cup, or WNBA)",
        "Channel": "What channel did this exposure come from? (Social, Digital, Broadcast, or Earned PR)",
        "Asset_Name": "What asset or branding was the exposure of? For Social: partnership mentions vs signage exposure; for Earned PR: partnership or product mentions vs signage exposure",
        "Seasonal_Event_Name": "During which part of the season did this exposure occur? (Week number, tentpole event, or program — must match Contractual dataset)",
        "Social_Platform": "What platform did the social exposure occur on? (Facebook, Instagram, Twitter, TikTok)",
        "Social_Account": "What social account posted the exposure? (Only for Social)",
        "Account_Handle": "The @ handle associated with the social account posting the exposure",
        "Unique_Social_ID": "Unique social post identifier (concatenation of date, account, and URL)",
        "Digital_Source": "What website posted the article that included the exposure? (Only for Digital)",
        "Unique_Digital_ID": "Unique digital article identifier (concatenation of date, source, article title, and URL)",
        "Media_Type": "Was this exposure in photo or video content? (Broadcast = video, Social/Digital can vary)",
        "URL": "Link to the social post or article that contained the exposure",
        "Market": "Was this regional or national? (Regional = not broadcasted nationally, National = broadcasted nationally)",
        "Unique_Broadcast_ID": "Unique broadcast program identifier (concatenation of exposure date, broadcaster, seasonal event, and home team if applicable)",
        "Broadcast_Network_(US)": "What national US TV broadcaster aired the exposure? (Only for US Broadcast)",
        "Broadcast_Network_(Local_H)": "What local US TV broadcaster aired the exposure? (Home Team, only for US Broadcast)",
        "Broadcast_Network_(Local_A)": "What local US TV broadcaster aired the exposure? (Away Team, only for US Broadcast)",
        "Broadcast_Network_(UK)": "What national UK TV broadcaster aired the exposure? (Only for UK Broadcast)",
        "Broadcast_Network_(SP)": "What national Spain TV broadcaster aired the exposure? (Only for Spain Broadcast)",
        "Total_Impressions": "How many total impressions did the exposure generate across the relevant channel?",
        "Social_Impressions": "How many total impressions did the social exposure garner? (Estimated, only for Social)",
        "Digital_Impressions": "How many total impressions did the digital exposure garner? (Only for Digital)",
        "Broadcast_Impressions_(US|HHLD)": "How many household impressions did the US broadcast exposure garner? (Only for US Broadcast)",
        "Broadcast_Impressions_(US National|P2+)": "How many total impressions did the national US broadcast exposure garner? (Only for US Broadcast)",
        "Broadcast_Impressions_(US Local H|P2+)": "How many total impressions did the home local US broadcast exposure garner? (Only for locally aired US Broadcasts)",
        "Broadcast_Impressions_(US Local A|P2+)": "How many total impressions did the away local US broadcast exposure garner? (Only for locally aired US Broadcasts)",
        "Broadcast_Impressions_(UK|P2+)": "How many total impressions did the national UK broadcast exposure garner? (Only for UK Broadcast)",
        "Broadcast_Impressions_(SP|P2+)": "How many total impressions did the national Spain broadcast exposure garner? (Only for Spain Broadcast)",
        "Engagements": "How many times was the social post containing the exposure engaged with?",
        "Video_Views": "How many video views did the social post containing the exposure garner? (Estimated)",
        "Exposures": "How many distinct times did branding appear? (Broadcast = 1, Social/Digital can vary; higher counts may account for extrapolated unscanned content)",
        "Video_Length": "How many seconds was the entire video?",
        "Duration_per_Exposure": "How many seconds was the exposure visible? (Social/Digital photos = 0)",
        "30_Sec_Equivalent": "Total exposure time divided by 30",
        "Duration_Factor": "For Social/Digital video, percentage of the video containing the exposure",
        "EXT_Factor": "Multiplier applied to account for unscanned Social/Digital content",
        "ME_Score": "What percentage of the content’s total value is attributable to Microsoft (prominence of branding)?",
        "ME_Value": "Also considered as MEV, Media equivalency value based on ad rates, duration, and ME score",
        "Home_Team": "Home team name (Only for broadcasted games)",
        "Away_Team": "Away team name (Only for broadcasted games)",
        "Brand_1": "Primary Microsoft brand focus (Copilot, Surface, Windows, Azure, Teams)",
        "Brand_2": "Secondary Microsoft brand focus, if applicable (Copilot, Surface, Windows, Azure, Teams)",
        "Content_Message": "Post copy (for Social) or article title (for Digital)",
        "Customer_Journey_Stage": "To be populated by Microsoft",
        "CSA_Targeted": "To be populated by Microsoft",
        "Audience_Persona": "To be populated by Microsoft"
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



table_agent_functions: List[Callable[..., Any]] = [execute_sql, get_table_schema]
import pandas as pd
import numpy as np
import requests
from requests.auth import HTTPBasicAuth

# 🟢 **Step 1: TM1 API Setup**
tm1_url = "http://localhost:52670/api/v1"  # Update if TM1 runs on a different host/port
username = "admin"
password = "apple"

session = requests.Session()
session.auth = HTTPBasicAuth(username, password)

mdx_query = {
    "MDX": """
SELECT 
    NON EMPTY {[Version].[Actual], [Version].[Budget]} ON COLUMNS, 
    NON EMPTY 
        TM1FilterByLevel( TM1SubsetAll( [Year] ), 0) * 
        TM1FilterByLevel( TM1SubsetAll( [Organization] ), 0) ON ROWS 
FROM [Revenue]
WHERE ([Month].[Year], [Channel].[Channel Total], [Product].[Phones], [Revenue].[Volume - Units])
   """
}

# 🟢 **Step 3: Execute MDX Query**
response = session.post(f"{tm1_url}/ExecuteMDX", json=mdx_query)

# Check if request was successful (201 Created)
if response.status_code != 201:
    print(f"Error executing MDX: {response.status_code}, {response.text}")
    exit()

cellset_id = response.json()["ID"]  # Extract Cellset ID

# 🟢 **Step 4: Fetch Axis 1 (Rows - Year & Organization)**
axis_1_response = session.get(f"{tm1_url}/Cellsets('{cellset_id}')/Axes(1)/Tuples/?$expand=Members"))
axis_1_data = axis_1_response.json()["value"]

axis_1_tuples = []
for tuple_entry in axis_1_data:
    members = tuple_entry["Members"]
    year = members[0]["Name"]  # Extract Year
    organization = members[1]["Name"]  # Extract Organization
    axis_1_tuples.append([year, organization])

# 🟢 **Step 5: Fetch Axis 0 (Columns - Version Names)**
axis_0_response = session.get(f"{tm1_url}/Cellsets('{cellset_id}')/Axes(0)/Tuples")
axis_0_data = axis_0_response.json()["value"]
####column_names = [member["Name"] for member in axis_0_data[0]["Members"]]  # Extract column names

# 🟢 **Step 6: Fetch Cell Values (Numbers)**
cells_response = session.get(f"{tm1_url}/Cellsets('{cellset_id}')/Cells")
cell_values = [cell["Value"] for cell in cells_response.json()["value"]]


df = pd.DataFrame(axis_1_tuples, columns=["Year", "Organization"])

# Convert Cell Values to Numpy Array (reshape into 70 rows, 2 columns)
cell_values_array = np.array(cell_values).reshape(len(axis_1_tuples), 2)

# Add Values to DataFrame
df["Actual"] = cell_values_array[:, 0]
df["Budget"] = cell_values_array[:, 1]



##############
import snowflake.connector

# Snowflake connection details
conn = snowflake.connector.connect(
    user="Khalid0204",
    password="@Snowflake123S",
    account="UJRNAQZ-WB06401",  # Example: xy12345.us-east-1
    warehouse="COMPUTE_WH",
    database="TM1POC",
    schema="TM1"
)

cursor = conn.cursor()

####################
create_table_query = """
CREATE TABLE IF NOT EXISTS Revenue (
    year VARCHAR,
    organization VARCHAR,
    actual FLOAT,
    budget FLOAT
);
"""

# Execute the SQL command
cursor.execute(create_table_query)

print("Table created successfully!")

#################
for index, row in df.iterrows():
    sql = f"""
    INSERT INTO Revenue (year, organization, actual, budget)
    VALUES ('{row['Year']}', '{row['Organization']}', {row['Actual']}, {row['Budget']})
    """
    cursor.execute(sql)
    conn.commit()


##############################################################################################################

Planning and Forecasting with Snowflake & TM1
Scenario Modeling: TM1 supports multi-dimensional modeling, which can be useful for planning and forecasting different business scenarios. These models can integrate data from Snowflake for more accurate financial predictions.
Dynamic Reporting: Snowflake’s SQL-based queries can be combined with TM1 models to dynamically generate reports that reflect both historical data from Snowflake and the current budget or forecast from TM1.
Advanced Analytics: Use Snowflake’s powerful data processing capabilities to run advanced analytics on the historical data, and use that data to refine your TM1 forecasts, creating a feedback loop that improves the accuracy of future forecasts.
4. Advanced Use Cases
Machine Learning Integration: Snowflake can integrate with various ML frameworks (such as AWS SageMaker, Google AI Platform, or Azure ML). You can use historical data in Snowflake to train models, and then apply the insights to refine your TM1 financial models.
Real-time Analytics: Integrating Snowflake with streaming services (like Kafka or Snowpipe) ensures that your TM1 system receives the most up-to-date information, leading to more accurate real-time planning and forecasting.
Example Workflow: Financial Forecasting
Data Collection: Financial data (revenue, expense, market trends) is stored in Snowflake.
Data Preparation: Use Snowflake to process and aggregate the data, ensuring it’s ready for analysis.
Modeling in TM1: Financial data and planning assumptions (like tax rates, growth percentages) are input into TM1’s multidimensional model for budgeting and forecasting.
Reporting and Forecasting: Using BI tools connected to Snowflake, generate dynamic reports and forecasts.
Refinement: Refine future budgets and forecasts in TM1 based on the insights derived from Snowflake.
Benefits of Integrating Snowflake with TM1 for Planning:
Scalability: Snowflake's architecture scales easily, allowing for the processing of massive datasets without impacting performance.
Data Consolidation: All data, from external systems to operational data, can be centralized in Snowflake, ensuring that planning models in TM1 use the most accurate and comprehensive data.
Real-time Planning: Snowflake’s ability to handle real-time data can help keep TM1 models up-to-date with the latest business conditions, improving the accuracy of forecasts and budgets.
Conclusion
Integrating IBM Planning Analytics (TM1) with Snowflake offers a powerful, scalable solution for financial planning, budgeting, and forecasting. It enables organizations to combine advanced financial modeling with powerful cloud-based data analytics, providing better decision-making insights and improving business performance.

Let me know if you'd like further details or a deeper dive into any specific integration step!


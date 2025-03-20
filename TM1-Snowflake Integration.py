import pandas as pd
import numpy as np
import requests
from requests.auth import HTTPBasicAuth
import json

# 🟢 **Step 1: TM1 API Setup**
tm1_url = "http://localhost:52670/api/v1"  # Update if TM1 runs on a different host/port
username = "admin"
password = "apple"

session = requests.Session()
session.auth = HTTPBasicAuth(username, password)

# 🟢 **Step 2: Data extract using MDX**
mdx_query = {
    "MDX": """
SELECT 
    NON EMPTY {[Version].[Actual], [Version].[Budget]} ON COLUMNS, 
    NON EMPTY 
        TM1FilterByLevel( TM1SubsetAll( [Year] ), 0) * 
        TM1FilterByLevel( TM1SubsetAll( [Organization] ), 0) ON ROWS 
FROM [Revenue]
WHERE ([Month].[Jan], [Channel].[Retail], [Product].[3G 32Gb], [Revenue].[Volume - Units])
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
axis_1_response = session.get(f"{tm1_url}/Cellsets('{cellset_id}')/Axes(1)/Tuples?$expand=Members")
print(axis_1_response.json())
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

# 🟢 **Step 6: Fetch Cell Values (Numbers)**
cells_response = session.get(f"{tm1_url}/Cellsets('{cellset_id}')/Cells")
cell_values = [cell["Value"] for cell in cells_response.json()["value"]]


df = pd.DataFrame(axis_1_tuples, columns=["Year", "Organization"])

# Convert Cell Values to Numpy Array (reshape into 70 rows, 2 columns)
cell_values_array = np.array(cell_values).reshape(len(axis_1_tuples), 2)

# Add Values to DataFrame
df["Actual"] = cell_values_array[:, 0]
df["Budget"] = cell_values_array[:, 1]
df["Channel"]="Retail"
df["Product"] ="3G 32Gb"
df["Month"] ="Jan"
df["Measure"] ="Volume - Units"
order=["Organization","Channel","Product","Month","Year","Actual","Budget", "Measure"]
df=df[order]



##############
import snowflake.connector

# Snowflake connection details
conn = snowflake.connector.connect(
    user= <User_Name>,
    password= <Password>,
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

#### Create a new column : Adjusted Budget##
ALTER TABLE REVENUE ADD COLUMN adjusted_budget DECIMAL(10,2);
UPDATE Revenue 
SET adjusted_budget = actual * 1.2;




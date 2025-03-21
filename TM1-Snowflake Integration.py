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
        {TM1FILTERBYPATTERN( {TM1FILTERBYLEVEL( {TM1SUBSETALL( [Year] )}, 0)}, "202*")} * 
        {TM1FILTERBYPATTERN( {TM1FILTERBYLEVEL( {TM1SUBSETALL( [organization] )}, 0)}, "10*")} ON ROWS 
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
#print(axis_1_response.json())
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
df["Product"] ="21002"
df["Month"] ="Jan"
df["Measure"] ="Volume - Units"
order=["Organization","Channel","Product","Month","Year", "Measure","Actual","Budget"]
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
    Organization VARCHAR,
    Channel VARCHAR,
    Product VARCHAR,
    Month VARCHAR,
    Year VARCHAR,
    Measure VARCHAR,
    Actual FLOAT,
    Budget FLOAT
);
"""

# Execute the SQL command
cursor.execute(create_table_query)

print("Table created successfully!")

#################
for index, row in df.iterrows():
    sql = f"""
    INSERT INTO Revenue (Organization, Channel, Product,Month,Year,Measure,Actual,Budget)
    VALUES ('{row['Organization']}', '{row['Channel']}', '{row['Product']}', '{row['Month']}','{row['Year']}','{row['Measure']}','{row['Actual']}','{row['Budget']}')
    """
    cursor.execute(sql)
    conn.commit()

#### Create a new column : Adjusted Budget##
sql_alter = f"""ALTER TABLE REVENUE ADD COLUMN adjusted_budget DECIMAL(10,2)
"""
######## Do some calculation #####
sql_update = f"""UPDATE Revenue 
SET adjusted_budget = actual * 1.2;
"""
cursor.execute(sql_alter)
cursor.execute(sql_update)

########## Write Back data to TM1 ########
## Setup TM1Py connection ###
from TM1py import TM1Service

# TM1 Connection Details
tm1 = TM1Service(
    address="localhost",
    port=52670,
    user="admin",
    password="apple",
    ssl=False
)
print("Connected to TM1:", tm1.server.get_product_version())
## select data from snowflake table##
sql_Write = f"""Select Organization, Channel, Product, Month, Year, Measure, Adjusted_Budget from REVENUE
"""
cursor.execute(sql_Write)
df_WR = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
df_WR["VERSION"]="Budget"
df_Final = pd.DataFrame({
    "Organization": df_WR["ORGANIZATION"],
	"Channel":df_WR["CHANNEL"],
	"Product":df_WR["PRODUCT"],
    "Month": df_WR["MONTH"],
	"Year": df_WR["YEAR"],
	"Version": df_WR["VERSION"],
	"Measure": df_WR["MEASURE"],
    "Value": df_WR["ADJUSTED_BUDGET"]
})
# Convert DataFrame to Dictionary
data = {
    tuple(row[:-1]): row[-1] for row in df_Final.values
}
tm1.cubes.cells.write_values("Revenue", data)

print("Data from DataFrame written successfully!")


______________Alternate___________
df_Final = pd.DataFrame({
    "Organization": ["101", "101", "101"],
	"Channel":["Retail","Retail","Retail"],
	"Product":["21002","21002","21002"],
    "Month": ["Jan", "Feb", "Mar"],
	"Year": ["2024", "2024", "2024"],
	"Version": ["Budget","Budget","Budget"],
	"Measure": ["Volume - Units", "Volume - Units", "Volume - Units"],
    "Value": [140, 160, 170]
})

# Convert DataFrame to Dictionary
data = {
    tuple(row[:-1]): row[-1] for row in df_Final.values
}
tm1.cubes.cells.write_values("Revenue", data)

print("Data from DataFrame written successfully!")






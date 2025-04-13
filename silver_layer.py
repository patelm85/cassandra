import pandas as pd
import uuid
from astrapy import DataAPIClient


#Configuration
ASTRA_DB_API_ENDPOINT = "https://46b80db0-0578-4780-94ab-3cb13487def6-us-east-2.apps.astra.datastax.com"
ASTRA_DB_TOKEN = "AstraCS:chPmRlnmESGGJZWOnMfDNxuh:879e33ee5b259438fee2a4c9204b5e2175a0833c1894734fb08bc65f0bb12e68"


#Initialize Astra DB Client
client = DataAPIClient(ASTRA_DB_TOKEN)
db = client.get_database_by_api_endpoint(ASTRA_DB_API_ENDPOINT)


#Load CSV Data
csv_path = "./data/sales_100.csv"
df = pd.read_csv(csv_path)
df.columns = df.columns.str.strip()  # Strip spaces from columns


#Create Silver Collection
silver_collection_name = "silver_sales"
if silver_collection_name not in db.list_collection_names():
    silver_collection = db.create_collection(silver_collection_name)
else:
    silver_collection = db.get_collection(silver_collection_name)


#Clean and Insert Data into Silver Collection
df_cleaned = df.drop_duplicates()
df_cleaned.fillna({
    'UnitsSold': 0,
    'UnitPrice': 0.0,
    'UnitCost': 0.0,
    'TotalRevenue': 0.0,
    'TotalCost': 0.0,
    'TotalProfit': 0.0
}, inplace=True)

inserted_silver = 0
for _, row in df_cleaned.iterrows():
    doc = {
        "_id": str(uuid.uuid4()),
        "region": row["Region"],
        "country": row["Country"],
        "item_type": row["Item Type"],
        "sales_channel": row["Sales Channel"],
        "order_priority": row["Order Priority"],
        "order_date": row["Order Date"],
        "order_id": row["Order ID"],
        "ship_date": row["Ship Date"],
        "units_sold": row["UnitsSold"],
        "unit_price": row["UnitPrice"],
        "unit_cost": row["UnitCost"],
        "total_revenue": row["TotalRevenue"],
        "total_cost": row["TotalCost"],
        "total_profit": row["TotalProfit"]
    }
    silver_collection.insert_one(doc)
    inserted_silver += 1

print(f"✅ Inserted {inserted_silver} documents into Silver Collection.")

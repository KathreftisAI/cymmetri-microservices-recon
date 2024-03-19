import pymongo
import json
import redis

# Redis configurations
REDIS_HOST = '10.0.1.7'
REDIS_PORT = 6379
REDIS_USERNAME = 'infra'
REDIS_PASSWORD = 'infra@123'
REDIS_DATABASE = 1

# Key to fetch data for
key = 'cymmetri-datascience_sh_recon_65f16ea71eea4028c1fa4c32_f595e9a3-77dd-476a-a424-3336552e3bac'

# Connect to Redis
redis_client = redis.StrictRedis(host=REDIS_HOST,
                                 port=REDIS_PORT,
                                 username=REDIS_USERNAME,
                                 password=REDIS_PASSWORD,
                                 db=REDIS_DATABASE,
                                 decode_responses=True)

# Fetch data for the key
data = redis_client.hgetall(key)
print(type(data))

# Check if data is found
if data is not None:
    print("Data for key '{}' found: {}".format(key, data))
else:
    print("No data found for key '{}'".format(key))

import json

# Assuming 'data' contains the fetched data from Redis
cymmetri_logins_dict = json.loads(data['Cymmetri_logins'])
final_app_overdue_dict = json.loads(data['final_app_overdue'])
appName = data["appName"]
reconReportMetadataId = data["reconReportMetadataId"]
appId = data["appId"]


# Create a new dictionary for Final App Overdue in the desired format
# cymmetri_logins_formatted = {"Cymmetri Logins":cymmetri_logins_dict}
# final_app_overdue_formatted = {'Final App Overdue': final_app_overdue_dict}

print("Cymmetri_Logins",cymmetri_logins_dict)
print("Final_Overdue",final_app_overdue_dict)
print("AppName: ",appName)
print("reconReportMetadataId: ",reconReportMetadataId)
print("appId",appId)

# Connect to MongoDB
client = pymongo.MongoClient("mongodb://unoadmin:devDb123@10.0.1.6:27019,10.0.1.6:27020,10.0.1.6:27021/?authSource=admin&replicaSet=sso-rs&retryWrites=true&w=majority")
db = client["cymmetri-datascience"]
syncDataForRecon = db["syncDataForRecon"]

# Initialize sets to store Reconciliation IDs and Logins
reconciliation_ids = set()
Target_logins = set()

# Set up the change stream to watch for new insertions
with syncDataForRecon.watch(full_document='updateLookup') as stream:
    for change in stream:
        if change["operationType"] == "insert":
            new_document = change["fullDocument"]
            reconciliation_id = new_document.get("reconciliationId")
            login = new_document.get("data", {}).get("login")
            if reconciliation_id and login:
                print("New Record Inserted:")
                # print("Reconciliation ID:", reconciliation_id)
                # print("Login:", login)
                # Append the values to the respective sets
                reconciliation_ids.add(reconciliation_id)
                Target_logins.add(login)
                print("Reconciliation IDs:", reconciliation_ids)
                print("Target_Logins:", Target_logins)

                # You can process or store the reconciliation ID and login as needed here

        # Print or do something with the sets after each iteration


    

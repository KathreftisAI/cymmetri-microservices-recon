from bson import ObjectId
from pymongo import MongoClient
import datetime
import redis
import json
from config.loader import Configuration
import platform

# MongoDB client initialization
client = MongoClient("mongodb://unoadmin:devDb123@10.0.1.6:27019,10.0.1.6:27020,10.0.1.6:27021/?authSource=admin&replicaSet=sso-rs&retryWrites=true&w=majority")
db = client["cymmetri-datascience"]

reconReportMetadata = db["reconReportMetadata"]
shRecordCymmetriAppLogins = db["shRecordCymmetriAppLogins"]
shReconBreakRecords = db["shReconBreakRecords"]


# # Initialize redis_client as None
# redis_client = None

# # Function to get Redis connection
# def get_redis_connection():
#     global redis_client
#     if redis_client is None:
#         if platform.system() == 'Windows':
#             config_path = "config.yaml"
#         else:
#             config_path = "/config/config.yaml"
#         c = Configuration(config_path)
#         data = c.getConfiguration()

#         host = data["REDIS_HOST"]
#         port = data["REDIS_PORT"]
#         username = data["REDIS_USERNAME"]
#         password = data["REDIS_PASSWORD"]
#         database = data["REDIS_DATABASE"]

#         conn_pool = redis.ConnectionPool(host=host, port=port, db=database, username=username, password=password)
#         redis_client = redis.Redis(connection_pool=conn_pool)

#     return redis_client

# # Call get_redis_connection to initialize redis_client
# redis_client = get_redis_connection()

# key = 'cymmetri-datascience_sh_recon_65fc04ae60643f5a285a06c6_3d660aa5-4eae-427b-bcb7-5792e9f90b77'

# # Check if redis_client is initialized before using it
# if redis_client:
#     data = redis_client.hgetall(key)
#     decoded_data = {key.decode(): value.decode() for key, value in data.items()}
#     print("data:", decoded_data)
# else:
#     print("Redis client is not initialized.")


# reconReportMetadataId = decoded_data.get('reconReportMetadataId')
# reconciliationId = decoded_data.get("reconciliationId")
# AppName =  decoded_data.get("appName")
# appId = decoded_data.get("appId")
# batchId = decoded_data.get("batchId")

# print("reconReportMetadataId:", reconReportMetadataId)
# print("reconciliationId: ",reconciliationId)
# print("appName: ",AppName)
# print("appId: ",appId)


# # Query MongoDB collection
# cursor = shRecordCymmetriAppLogins.find({"reconReportMetadataId": reconReportMetadataId})

# # Iterate over the cursor to print Target_logins\
# shRecordCymmetriAppLogins = set()
# for document in cursor:
#     target_logins = document.get("Target_logins", [])
#     shRecordCymmetriAppLogins.add(target_logins)


# print("shRecordCymmetriAppLogins:", shRecordCymmetriAppLogins)


# key1 = "65fc04ae60643f5a285a06c6"

# if redis_client:
#     data1 = redis_client.get(key1)
#     decoded_data1 = data1.decode('utf-8')
#     target_logins_set = set(json.loads(decoded_data1))
# else:
#     print("Redis Client is not initilaized")

# print("Target_Logins:", target_logins_set)





# only_present_in_Cymmetri = shRecordCymmetriAppLogins - target_logins_set

# print("only_present_in_Cymmetri:", only_present_in_Cymmetri)


# #insert data in mongo collection shReconBreakRecords

    
# for login in only_present_in_Cymmetri:
#     record = {
#         "reconciliationId": reconciliationId,
#         "AppName": AppName,
#         "appId": appId,
#         "Cymmetri_login": login,
#         "breakType": "only_present_in_Cymmetri",
#         "performedAt": datetime.datetime.now(),
#         "reconReportMetadataId": reconReportMetadataId
#     }
#     shReconBreakRecords.insert_one(record)
#     print("Inserted record for Cymmetri_login:", login)


break_types = ["break_type_in_target_only", "only_present_in_Cymmetri","break_type_overdue"]

# Initialize a dictionary to store the counts
break_type_counts = {}

# Iterate over the break types and count the documents for each type
for break_type in break_types:
    count = shReconBreakRecords.count_documents({"breakType": break_type})
    break_type_counts[break_type] = count

# Print the counts
for break_type, count in break_type_counts.items():
    print(f"Break Type: {break_type}, Count: {count}")



from pymongo import MongoClient
from datetime import datetime
import redis 
import json 
from pymongo import MongoClient
from datetime import datetime

REDIS_HOST = '10.0.1.7'
REDIS_PORT = 6379
REDIS_USERNAME = 'infra'
REDIS_PASSWORD = 'infra@123'
REDIS_DATABASE = 1

# Key for Redis hashmap
redis_key = '65fc04ae60643f5a285a06c6'

# Connect to Redis
redis_client = redis.StrictRedis(host=REDIS_HOST,
                                 port=REDIS_PORT,
                                 username=REDIS_USERNAME,
                                 password=REDIS_PASSWORD,
                                 db=REDIS_DATABASE,
                                 decode_responses=True)

# Fetch hash data from Redis
redis_client = redis.StrictRedis(host=REDIS_HOST,
                                 port=REDIS_PORT,
                                 username=REDIS_USERNAME,
                                 password=REDIS_PASSWORD,
                                 db=REDIS_DATABASE,
                                 decode_responses=True)

# Fetch string data from Redis
string_data = redis_client.get(redis_key)

# Convert the JSON-formatted string to a set
if string_data:
    target_logins_list = json.loads(string_data)
    Target_logins = set(target_logins_list)
    print("Target_logins",Target_logins)
else:
    print("No data found in Redis for the given key.")



redis_key = 'cymmetri-datascience_sh_recon_65f8430941fd647553ffbcea_f699c7a0-a744-4f14-8272-6aecf7a66b8d'

# Connect to Redis
redis_client = redis.StrictRedis(host=REDIS_HOST,
                                 port=REDIS_PORT,
                                 username=REDIS_USERNAME,
                                 password=REDIS_PASSWORD,
                                 db=REDIS_DATABASE,
                                 decode_responses=True)


# Fetch hash data from Redis
hash_data = redis_client.hgetall(redis_key)

# Extract required fields
cymmetri_logins = json.loads(hash_data.get("Cymmetri_logins", "[]"))
Final_Overdue = json.loads(hash_data.get("final_app_overdue", "[]"))

# Convert final_app_overdue to set
#final_app_overdue_set = {item["cymmetriLogin"] for item in final_app_overdue}

print("Cymmetri_logins:", cymmetri_logins)
print("Final App Overdue Set:", Final_Overdue)




# # break_type_in_cymmetri_only = False
# # break_type_in_target_only = False
# # break_type_overdue = False

AppName =   "Active Directory"
reconReportMetadataId =  "65fc35eea58da1531da9f8a9"
appId =  "653f482a0d2e4b5f289727ff"
reconciliationId = "65fc04ae60643f5a285a06c6"


Cymmetri_Logins = set(cymmetri_logins)

# # # Find common logins between Logins set and cymmetri_logins_set
# # common_logins = Target_logins.intersection(cymmetri_logins_set)

# # break_type_in_target_only = Target_logins - cymmetri_logins_set
# # break_type_in_cymmetri_only = cymmetri_logins_set - Target_logins

# # print("break_type_in_target_only: ",break_type_in_target_only)
# # print("break_type_in_cymmetri_only: ",break_type_in_cymmetri_only)


#find the app_overdue 
# app_overdue_logins_cymmetri = []
# for item in Final_Overdue:
#     #print("Cymmetri_Logins",item["cymmetriLogin"])
#     app_overdue_logins_cymmetri.append(item['cymmetriLogin'])

# print("app_overdue_logins_cymmetri",app_overdue_logins_cymmetri)

# app_overdue_logins_target = []
# for item in Final_Overdue:
#     #print("App_Logins",item["appLogin"])
#     app_overdue_logins_target.append(item['appLogin'])

# print("app_overdue_logins_target",app_overdue_logins_target)

# break_type_overdue =  Target_logins.intersection(app_overdue_logins_cymmetri) 
# print("break_type_overdue",break_type_overdue)




def find_break_types_and_insert(target_logins, cymmetri_logins, final_overdue):
    # Find break_type_in_target_only
    break_type_in_target_only = target_logins - set(cymmetri_logins)
    
    # Find break_type_in_cymmetri_only
    break_type_in_cymmetri_only = set(cymmetri_logins) - target_logins
    
    # Find app_overdue_logins_cymmetri
    app_overdue_logins_cymmetri = [item['cymmetriLogin'] for item in final_overdue]
    
    # Find app_overdue_logins_target
    app_overdue_logins_target = [item['appLogin'] for item in final_overdue]
    
    # Find break_type_overdue
    break_type_overdue = set(target_logins).intersection(app_overdue_logins_cymmetri)
    print("break_type_overdue",break_type_overdue)
    
    # Connect to MongoDB
    client = MongoClient("mongodb://unoadmin:devDb123@10.0.1.6:27019,10.0.1.6:27020,10.0.1.6:27021/?authSource=admin&replicaSet=sso-rs&retryWrites=true&w=majority")
    db = client["cymmetri-datascience"]
    collection = db["shReconBreakRecords"]
    
    # Insert records for break_type_in_target_only
    for login in break_type_in_target_only:
        record = {
            "reconciliationId": "65f16ea71eea4028c1fa4c32",
            "AppName": "Active Directory",
            "appId": "653f482a0d2e4b5f289727ff",
            "targetAppLogin": login,
            "breakType": "break_type_in_target_only",
            "performedAt": datetime.utcnow(),
            "reconReportMetadataId": "65f416c178b1347f40aabbbd"
        }
        collection.insert_one(record)
    
    # Insert records for break_type_in_cymmetri_only
    for login in break_type_in_cymmetri_only:
        record = {
            "reconciliationId": "65f16ea71eea4028c1fa4c32",
            "AppName": "Active Directory",
            "appId": "653f482a0d2e4b5f289727ff",
            "CymmetriAppLogin": login,
            "breakType": "break_type_in_cymmetri_only",
            "performedAt": datetime.utcnow(),
            "reconReportMetadataId": "65f416c178b1347f40aabbbd"
        }
        collection.insert_one(record)
    
    # Insert records for break_type_overdue
    for login in break_type_overdue:
        record = {
            "reconciliationId": "65f16ea71eea4028c1fa4c32",
            "AppName": "Active Directory",
            "appId": "653f482a0d2e4b5f289727ff",
            "targetAppLogin": login,
            "breakType": "break_type_overdue",
            "performedAt": datetime.utcnow(),
            "reconReportMetadataId": "65f416c178b1347f40aabbbd"
        }
        collection.insert_one(record)

# Call the function with the required inputs
find_break_types_and_insert(Target_logins, Cymmetri_Logins, Final_Overdue)





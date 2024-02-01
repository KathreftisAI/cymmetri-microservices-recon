# user present in target App but not in Cymmetri

from pymongo import MongoClient
from bson import ObjectId
import pymongo
from pymongo import DESCENDING
import datetime
from datetime import datetime

# Connect to MongoDB
client = MongoClient("mongodb://unoadmin:devDb123@10.0.1.6:27019,10.0.1.6:27020,10.0.1.6:27021/?authSource=admin&replicaSet=sso-rs&retryWrites=true&w=majority")
db = client['newqa-recontestsahil'] 
db_user = client["user"]


'''
Task 1 - Loop over all users, get all distinct provisionedApps.<appId>.
remove CYMMETRI from this list

For each such appId - find in reconciliationPull and reconciliationPush, the 
corresponding reconId 

such that - 

appId key --> 
value is a list of dict --> 
[type:"USER"/"GROUP"] --> [reconId:""] --> [PUSH/PULL] 


for each such reconId where type-> USER
get batchId

Process each such batchId to get corresponding breaktype (push/pull give different breaks)

'''


'''
Task 2- 

For each break instance - 

1. specify which break type
2. UserId (wherever present in cymmetri or target app or both)
3. Give other details as necessary -> e.g., if in cymmetri but not target app
 mention cymmetri login id, and which app was checked
4. When was this break check run and break was found.
5. Store in mongodb as collection "recon_break_results" 

'''

pipeline = [
    {
        "$project": {
            "provisionedApps": {
                "$objectToArray": "$provisionedApps"
            }
        }
    },
    {
        "$unwind": "$provisionedApps"
    },
    {
        "$group": {
            "_id": "$provisionedApps.k",
        }
    }
]

# Execute the pipeline on the user collection in the newqa-recontestsahil database
distinct_apps_user = db.user.aggregate(pipeline)

# Extract the distinct appIds from the result
distinct_app_ids_user = [app['_id'] for app in distinct_apps_user]

# Remove "CYMMETRI" from the list of distinct appIds if it exists
if "CYMMETRI" in distinct_app_ids_user:
    distinct_app_ids_user.remove("CYMMETRI")

print("Distinct appIds from newqa-recontestsahil (user collection):", distinct_app_ids_user)


# for app_id in distinct_app_ids_user:
#     matching_records = db.reconciliationPull.find({"applicationId": app_id})

#     for records in matching_records:
#         print(f"this {app_id} app_id from user is matching",records)

# Create an empty dictionary to store matching records
matching_records_dict = {}

for app_id in distinct_app_ids_user:
    matching_records = db.reconciliationPull.find({"applicationId": app_id})

    # Store records in the dictionary, using app_id as the key
    matching_records_dict[app_id] = [record for record in matching_records]

# Print the resulting dictionary
for app_id, records in matching_records_dict.items():
    print(f"Records for app_id {app_id}:", records)


user_type_ids_set = set()

for app_id in distinct_app_ids_user:
    matching_records = db.reconciliationPull.find({"applicationId": app_id})

    for record in matching_records:
        # Check if the 'type' is 'USER' and extract _id
        if record.get('type') == 'USER':
            # Add the _id value (as string) to the set
            user_type_ids_set.add(str(record.get('_id')))

# Print the set of _id values for 'type': 'USER'
print("User Type _id values:", user_type_ids_set)

user_type_count = {}

# Iterate over each user_type_id in the set
for user_type_id in user_type_ids_set:
    # Find matching records in syncData for the reconciliationId
    matching_sync_data = db.syncData.find({"reconciliationId": user_type_id})

    # Initialize count for the current user_type_id
    count = 0
    
    # Iterate over matching records in syncData
    for sync_record in matching_sync_data:
        count += 1
        #print("Matching record in syncData:", sync_record)

    # Store the count in the dictionary
    user_type_count[str(user_type_id)] = count

# Print the user_type_id and the count of matching records
for user_type_id, count in user_type_count.items():
    print(f"ReconciliationId {user_type_id}: {count} matching records in syncData")

batch_ids_info = {}

# Iterate over each user_type_id in the set
for user_type_id in user_type_ids_set:
    # Find matching records in syncData for the reconciliationId
    matching_sync_data = db.syncData.find({"reconciliationId": user_type_id})

    # Initialize variables to keep track of the latest createdDateTime and batchId
    latest_created_datetime = None
    latest_batch_id = None
    
    # Iterate over matching records in syncData
    for sync_record in matching_sync_data:
        # Check if createdDateTime is greater than the latest
        if latest_created_datetime is None or sync_record.get('createdDateTime') > latest_created_datetime:
            latest_created_datetime = sync_record.get('createdDateTime')
            latest_batch_id = sync_record.get('batchId')

    # Store the latest batchId info in the dictionary
    batch_ids_info[str(user_type_id)] = {
        'latest_batch_id': latest_batch_id,
        'latest_created_datetime': latest_created_datetime
    }

# Print the batchId information for each ReconciliationId
for user_type_id, info in batch_ids_info.items():
    print(f"\nReconciliationId {user_type_id}:")
    print(f"Latest BatchId: {info['latest_batch_id']}")
    print(f"Latest CreatedDateTime: {info['latest_created_datetime']}\n")





distinct_logins_target = set()

for user_type_id, info in batch_ids_info.items():
    latest_batch_id = info['latest_batch_id']
    # print(f"\nReconciliationId {user_type_id}:")
    # print(f"Latest BatchId: {latest_batch_id}")
    # print(f"Latest CreatedDateTime: {info['latest_created_datetime']}\n")

    matching_records = db.syncData.find({"batchId": latest_batch_id}, {"data.login": 1})

    for record in matching_records:
        login = record.get("data", {}).get("login")
        if login:
            distinct_logins_target.add(login)

# Print the distinct login values
print("Distinct Logins from Target Application:", distinct_logins_target, len(distinct_logins_target))


distinct_logins_from_user = set()

for app_id in distinct_app_ids_user:
    matching_records = db.user.find({"provisionedApps.{}.login.login".format(app_id): {"$exists": True}}, {"provisionedApps.{}.login.login".format(app_id): 1})

    for record in matching_records:
        login = record.get("provisionedApps", {}).get(app_id, {}).get("login", {}).get("login")
        if login:
            distinct_logins_from_user.add(login)

# Print the distinct login values from the user collection
print("Distinct Logins from Cymmetri Application:", distinct_logins_from_user, len(distinct_logins_from_user))

missing_values = distinct_logins_target - distinct_logins_from_user
print("Account Created Outside of Cymmetri",missing_values)

missing_values_details = []

# Iterate over each missing login
for missing_login in missing_values:
    # Find matching records in syncData for the login
    matching_records_sync_data = db.syncData.find({"data.login": missing_login})

    for sync_record in matching_records_sync_data:
        # Extract required details from syncData
        sync_data_details = {
            "batchId": sync_record.get("batchId"),
            "reconciliationId": sync_record.get("reconciliationId"),
            "login": missing_login,
            "Break_Type":"Account created outside Cymmetri",
            "PerformedAt": datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + "+00:00"
        }

        # Find matching record in reconciliationPull using reconciliationId
        matching_record_reconciliation_pull = db.reconciliationPull.find_one({"_id": ObjectId(sync_record.get("reconciliationId"))})

        if matching_record_reconciliation_pull:
            # Extract applicationId from reconciliationPull
            sync_data_details["applicationId"] = matching_record_reconciliation_pull.get("applicationId")

            # Append the details to the list
            missing_values_details.append(sync_data_details)
print("\n=====Final MongoDB Collection Summary=====\n")
# Print the details for each missing value
for details in missing_values_details:
    print(details)




# final_output = []

# for login in missing_values:
#     # Fetch applicationId from the reconciliationPull collection
#     reconciliation_pull_record = db.reconciliationPull.find_one({"data.login": login})
#     if reconciliation_pull_record:
#         application_id = reconciliation_pull_record.get("applicationId")
#     else:
#         application_id = None

#     # Fetch reconUuid, batchId from the syncData collection
#     sync_data_record = db.syncData.find_one({"data.login": login})
#     if sync_data_record:
#         recon_uuid = sync_data_record.get("reconUuid")
#         batch_id = sync_data_record.get("batchId")
#     else:
#         recon_uuid = None
#         batch_id = None

#     # Create the final output dictionary
#     output_entry = {
#         "login": login,
#         "applicationId": application_id,
#         "reconUuid": recon_uuid,
#         "batchId": batch_id,
#         "PerformedAt": datetime.datetime.utcnow(),
#         "Break_Type": "Break_1 Account Created Outside Cymmetri"
#     }

#     # Append the entry to the final_output list
#     final_output.append(output_entry)

# # Print the final JSON output
# print("Final Output for missing_values:")
# print(final_output)





# #Close the MongoDB connection
# client.close()


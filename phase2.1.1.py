import pymongo
import json
import redis
from datetime import datetime


# Redis configurations
REDIS_HOST = '10.0.1.7'
REDIS_PORT = 6379
REDIS_USERNAME = 'infra'
REDIS_PASSWORD = 'infra@123'
REDIS_DATABASE = 1


# Key for Redis hashmap
redis_key = '65f8430941fd647553ffbcea'

redis_client = redis.StrictRedis(host=REDIS_HOST,
                                 port=REDIS_PORT,
                                 username=REDIS_USERNAME,
                                 password=REDIS_PASSWORD,
                                 db=REDIS_DATABASE,
                                 decode_responses=True)





# Connect to MongoDB
client = pymongo.MongoClient("mongodb://unoadmin:devDb123@10.0.1.6:27019,10.0.1.6:27020,10.0.1.6:27021/?authSource=admin&replicaSet=sso-rs&retryWrites=true&w=majority")
db = client["cymmetri-datascience"]
syncDataForRecon = db["syncDataForRecon"]
shRecordCymmetriAppLogins = db["shRecordCymmetriAppLogins"]


key = 'cymmetri-datascience_sh_recon_65f8430941fd647553ffbcea_f699c7a0-a744-4f14-8272-6aecf7a66b8d'
recon_report_metadata_id = redis_client.hget(key, 'reconReportMetadataId')
reconciliationId = redis_client.hget(key,"reconciliationId")
batch_id = redis_client.hget(key, 'batchId')

# Print the values
print("reconReportMetadataId:", recon_report_metadata_id)
print("batchId:", batch_id)
print("reconciliationId",reconciliationId)


# Initialize sets to store Reconciliation IDs and Logins
reconciliation_ids = set()
Target_logins = set()

reconReportMetadataId = recon_report_metadata_id
batch_id = batch_id




# Set up the change stream to watch for new insertions
with syncDataForRecon.watch(full_document='updateLookup') as stream:
    for change in stream:
        if change["operationType"] == "insert":
            new_document = change["fullDocument"]
            reconciliation_id = new_document.get("reconciliationId")
            login = new_document.get("data", {}).get("login")
            if reconciliation_id and login:
                print("New Record Inserted:")
                # Append the values to the respective sets
                reconciliation_ids.add(reconciliation_id)  # Use add for sets
                Target_logins.add(login)
                print("Reconciliation IDs:", reconciliation_ids)
                print("Target_Logins:", Target_logins)
                
                # Convert Target_logins set to a list and serialize to JSON
                target_logins_list = list(Target_logins)
                target_logins_json = json.dumps(target_logins_list, ensure_ascii=False)
                
                # Store serialized JSON in Redis
                redis_client.set(redis_key, target_logins_json)




                for login in target_logins_list:
                    existing_document = shRecordCymmetriAppLogins.find_one({"Target_logins": login})
                    if existing_document:
                        # Check if reconciliation_ids is an array, if not, convert it to a set
                        if not isinstance(existing_document.get("reconciliation_ids"), list):
                            reconciliation_ids = set(existing_document.get("reconciliation_ids"))
                        else:
                            reconciliation_ids = set(existing_document.get("reconciliation_ids"))

                        # Add the new reconciliationId if it's not already present
                        if reconciliationId not in reconciliation_ids:
                            reconciliation_ids.add(reconciliationId)
                            
                            # Update the existing document
                            # Convert the set to a comma-separated string
                            reconciliation_ids_str = ','.join(reconciliation_ids)
                            shRecordCymmetriAppLogins.update_one({"_id": existing_document["_id"]}, {"$set": {"reconciliation_ids": reconciliation_ids_str}})
                    else:
                        # Get the syncDataForRecon _id based on the login
                        sync_data_document = syncDataForRecon.find_one({"data.login": login})
                        sync_data_recon_id = sync_data_document.get("_id") if sync_data_document else None
                        
                        # Insert a new document
                        data_to_insert = {
                            "reconciliation_ids": [reconciliationId],  # Store reconciliationId directly as a string
                            "batch_id": batch_id,
                            "reconReportMetadataId": recon_report_metadata_id,
                            "Target_logins": login,
                            "created": datetime.now(),
                            "syncdataReconId": sync_data_recon_id  # Add the syncdataReconId field
                        }
                        shRecordCymmetriAppLogins.insert_one(data_to_insert)
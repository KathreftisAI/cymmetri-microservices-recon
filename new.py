from pymongo import MongoClient
from bson import ObjectId
from datetime import datetime
import pytz

client = MongoClient("mongodb://unoadmin:devDb123@10.0.1.6:27019,10.0.1.6:27020,10.0.1.6:27021/?authSource=admin&replicaSet=sso-rs&retryWrites=true&w=majority")
db = client['cymmetri-datascience']
recon_report_metadata = db["reconReportMetadata"]

def get_indian_time():
    tz = pytz.timezone('Asia/Kolkata')
    return datetime.now(tz)

def missing_records(db):
    # Function to fetch logins excluding CYMMETRI
    def fetch_logins_exclude_cymmetri():
        # Initialize an empty list to store Cymmetri logins
        cymmetri_logins = []

        # Fetch reconciliationId from reconReportMetadata collection
        recon_metadata_collection = db['reconReportMetadata']

        # Retrieve reconciliation IDs
        recon_ids = [doc['reconciliationId'] for doc in recon_metadata_collection.find()]

        # Accessing reconciliationPull collection
        recon_pull_collection = db["reconciliationPull"]

        # Accessing user collection
        user_collection = db["user"]

        # Iterate through each reconciliation ID
        for recon_id in recon_ids:
            # Convert recon_id string to ObjectId
            recon_object_id = ObjectId(recon_id)

            # Find documents with the same recon_id
            matching_docs = recon_pull_collection.find({"_id": recon_object_id})

            for doc in matching_docs:
                appId = doc['applicationId']
                if appId != "CYMMETRI":
                    print("appID:", appId)

                    # Find user document with the given applicationId where login.login exists
                    user_docs = user_collection.find({"provisionedApps." + appId + ".login.login": {"$exists": True}})

                    # Iterate over found user documents
                    for user_doc in user_docs:
                        # Extract login.login value
                        login_login = user_doc["provisionedApps"][appId]["login"]["login"]
                        cymmetri_logins.append(login_login)

        # Print the list of login.logins values
        print("Cymmetri Logins:", cymmetri_logins)
        return cymmetri_logins

    # Function to fetch matching records
    def fetch_matching_records():
        # Initialize an empty list to store matching records
        matching_records = []
        target_logins = []

        # Accessing reconReportMetadata collection
        recon_metadata_collection = db['reconReportMetadata']

        # Accessing syncDataForRecon collection
        sync_data_collection = db['syncDataForRecon']

        # Iterate through documents in reconReportMetadata collection
        for recon_metadata_doc in recon_metadata_collection.find():
            batch_id = recon_metadata_doc.get('batchId')
            reconciliation_id = recon_metadata_doc.get('reconciliationId')

            # Find documents in syncDataForRecon collection with matching batchId and reconciliationId
            sync_data_docs = sync_data_collection.find({
                'batchId': batch_id,
                'reconciliationId': reconciliation_id
            })

            # Append matching records to the list
            for sync_data_doc in sync_data_docs:
                matching_records.append(sync_data_doc)
                # Fetch data.login value and store it in target_logins list
                target_logins.append(sync_data_doc['data']['login'])
            print("Traget Logins ",target_logins)

        return matching_records, target_logins

    # Function to insert missing logins into recon_break_type collection
    def insert_missing_logins(missing_logins):
        sync_data_collection = db['syncDataForRecon']
    
        # Accessing recon_break_type collection
        recon_break_type_collection = db['recon_break_type']
        
        # Accessing break_count collection
        break_count_collection = db['breakReportMetadata']

        # Update break count
        inserted_records_count = 0
        inserted_object_ids = []

        for index, missing_login in enumerate(missing_logins, start=1):
            # Find the corresponding document in syncDataForRecon collection
            sync_data_doc = sync_data_collection.find_one({'data.login': missing_login})
            
            if sync_data_doc:
                # Extract batchId and reconciliationId
                batch_id = sync_data_doc.get('batchId')
                reconciliation_id = sync_data_doc.get('reconciliationId')
                
                # Create document to insert into recon_break_type collection
                break_doc = {
                    'batchId': batch_id,
                    'reconciliationId': reconciliation_id,
                    'loginId': missing_login,
                    'breakType': 'Account created outside Cymmetri',
                    'performedAt': datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + "+00:00"
                }
                
                # Insert document into recon_break_type collection
                result = recon_break_type_collection.insert_one(break_doc)
                
                # Store the inserted object ID
                inserted_object_ids.append(result.inserted_id)
                
                # Increment the count of inserted records
                inserted_records_count += 1

            else:
                print(f"No syncDataForRecon document found for login: {missing_login}")

            # Print count of total missing values and count of inserted records
            print(f"Processed {index}/{len(missing_logins)} missing logins. {inserted_records_count} records inserted.")

        # Update break count document
        indian_time = get_indian_time()
        formatted_indian_time = indian_time.strftime('%Y-%m-%d %H:%M:%S %Z%z')

        break_count_document = {
            "status": f"{inserted_records_count} new records inserted",
            "performedAt": formatted_indian_time,
            "objectIDs": inserted_object_ids
        }
        break_count_collection.insert_one(break_count_document)

    # Call the functions sequentially
    cymmetri_logins = fetch_logins_exclude_cymmetri()
    matching_records, target_logins = fetch_matching_records()

    # Calculate missing logins
    missing_logins = set(target_logins) - set(cymmetri_logins)

    # Insert missing logins into recon_break_type collection
    insert_missing_logins(missing_logins)

# Call the function with your database connection
missing_records(db)
    
# def watch_recon_report_metadata():
#     with recon_report_metadata.watch(full_document='updateLookup') as stream:
#         for change in stream:
#             print("Change detected in reconReportMetadata collection.")
#             #print(change)
#             missing_records(db)
#             print("change is detected and function has run")

# if __name__ == "__main__":
#     watch_recon_report_metadata()

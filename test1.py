from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import JSONResponse
from pymongo import MongoClient
from bson import ObjectId
from datetime import datetime
import pytz
import uvicorn
import logging

logging.basicConfig(filename='app.log', level=logging.INFO)
client = MongoClient("mongodb://unoadmin:devDb123@10.0.1.6:27019,10.0.1.6:27020,10.0.1.6:27021/?authSource=admin&replicaSet=sso-rs&retryWrites=true&w=majority")

app = FastAPI()


def check_app_overdue(app_ids, cymmetri_logins):
    db = client["cymmetri-datascience"]
    current_utc_time = datetime.utcnow()
    print("current_utc_time: ",current_utc_time)
    matching_users = []
    messages = []

    for app_id in app_ids:
        print("app_id: ",app_id)
        print("appidsssss: ",app_ids)
        for cymmetri_login in cymmetri_logins:
            print("cymmetri_login: ",cymmetri_login)
            users = db.user.find({
                "end_date": {"$exists": True, "$lt": current_utc_time},
                "status": "ACTIVE",
                f"provisionedApps.{app_id}.login.login": cymmetri_login
            })
            print("jnjldnenedend")
            matching_users.extend(users)
    print("usersssss: ",matching_users)
    print(len(matching_users))
    for user in matching_users:
        user_id = str(user["_id"])
        display_name = user.get("displayName", "N/A")
        end_date = user.get("end_date", "N/A")
        for app_id, app_details in user.get("provisionedApps", {}).items():
            app_login = app_details.get("login", {}).get("login", "N/A")
            app_status = app_details.get("login", {}).get("status", "N/A")

            if app_status != "SUCCESS_DELETE" or "SUCCESS_UPDATE":
                message = (f"User ID: {user_id}, Display Name: {display_name}, "
                           f"App ID: {app_id}, App Status: {app_status}, "
                           f"Cymmetri Status: {user['status']}, "
                           f"End Date: {end_date}, Current Date: {current_utc_time}")
                messages.append(message)

    # Print each message only once
    for message in messages:
        print("Break Type:: App_Overdue")
        print(message)


@app.post("/missing_records")
async def missing_records(db_name: str = Header("cymmetri-datascience")):
    # Function to connect to the MongoDB client and return the specified database
    def connect_to_db(db_name):
        client = MongoClient("mongodb://unoadmin:devDb123@10.0.1.6:27019,10.0.1.6:27020,10.0.1.6:27021/?authSource=admin&replicaSet=sso-rs&retryWrites=true&w=majority")
        return client[db_name]

    # Function to get Indian time
    def get_indian_time():
        tz = pytz.timezone('Asia/Kolkata')
        return datetime.now(tz)

    # Function to fetch logins excluding CYMMETRI
    def fetch_logins_exclude_cymmetri(db):
        cymmetri_logins = set()
        app_ids = set()

        recon_metadata_collection = db['reconReportMetadata']
        recon_ids = [doc['reconciliationId'] for doc in recon_metadata_collection.find()]

        recon_pull_collection = db["reconciliationPull"]
        user_collection = db["user"]

        for recon_id in recon_ids:
            recon_object_id = ObjectId(recon_id)
            matching_docs = recon_pull_collection.find({"_id": recon_object_id})

            for doc in matching_docs:
                appId = doc['applicationId']
                if appId != "CYMMETRI":
                    app_ids.add(appId)

                    user_docs = user_collection.find({"provisionedApps." + appId + ".login.login": {"$exists": True}})
                    for user_doc in user_docs:
                        login_login = user_doc["provisionedApps"][appId]["login"]["login"]
                        cymmetri_logins.add(login_login)

        cymmetri_logins_list = list(cymmetri_logins)
        logging.info("Cymmetri Logins: %s", cymmetri_logins_list)
        return app_ids, cymmetri_logins_list

    # Function to fetch matching records
    def fetch_matching_records(db):
        matching_records = []
        target_logins = []

        recon_metadata_collection = db['reconReportMetadata']
        sync_data_collection = db['syncDataForRecon']

        for recon_metadata_doc in recon_metadata_collection.find():
            batch_id = recon_metadata_doc.get('batchId')
            reconciliation_id = recon_metadata_doc.get('reconciliationId')

            sync_data_docs = sync_data_collection.find({
                'batchId': batch_id,
                'reconciliationId': reconciliation_id
            })

            for sync_data_doc in sync_data_docs:
                matching_records.append(sync_data_doc)
                target_logins.append(sync_data_doc['data']['login'])

        print("Target Logins ", target_logins)

        return matching_records, target_logins

    # Function to insert missing logins into recon_break_type collection
    def insert_missing_logins(missing_logins,db):
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
            "objectIDs": [{"$oid": str(oid)} for oid in inserted_object_ids]  # Convert ObjectIDs to the desired format
        }
        break_count_collection.insert_one(break_count_document)


    db = connect_to_db(db_name)
    app_ids, cymmetri_logins = fetch_logins_exclude_cymmetri(db)
    matching_records, target_logins = fetch_matching_records(db)
    missing_logins = set(target_logins) - set(cymmetri_logins)

    check_app_overdue(app_ids, cymmetri_logins)

    insert_missing_logins(missing_logins, db)

    return JSONResponse(content={"message": "Missing records processed successfully."})


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=5000)

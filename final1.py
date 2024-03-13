from fastapi import FastAPI, HTTPException, Header
from pymongo import MongoClient
from bson import ObjectId
from datetime import datetime
import uvicorn
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import logging
from database.connection import get_collection
from config.loader import Configuration
import platform
import pytz

def get_indian_time():
    tz = pytz.timezone('Asia/Kolkata')
    return datetime.now(tz)

# MongoDB connection for newqa-recontestsahil
client = MongoClient("mongodb://unoadmin:devDb123@10.0.1.6:27019,10.0.1.6:27020,10.0.1.6:27021/?authSource=admin&replicaSet=sso-rs&retryWrites=true&w=majority")
# db = client['newqa-recontestsahil'] 
# user_collection = db["user"]
# reconciliationPull = db["reconciliationPull"]
# syncData = db["syncData"]
# break_count = db["break_count"]
# recon_break_type = db["recon_break_type"]
# recon_report_metadata = db["reconReportMetadata"]

#Mongo db connection for cymmetri-datascience
db = client["cymmetri-datascience"]
user_collection = db["user"]
reconciliationPull = db["reconciliationPull"]
syncData = db["syncDataForRecon"]
break_count = db["break_count"]
recon_break_type = db["recon_break_type"]
recon_report_metadata = db["reconReportMetadata"]

app = FastAPI()

def get_distinct_app_ids():
    pipeline = [
        {"$project": {"provisionedApps": {"$objectToArray": "$provisionedApps"}}},
        {"$unwind": "$provisionedApps"},
        {"$match": {"provisionedApps.k": {"$ne": "CYMMETRI"}}},  
        {"$group": {"_id": "$provisionedApps.k"}}
    ]
    distinct_apps_user = user_collection.aggregate(pipeline)
    return [app['_id'] for app in distinct_apps_user]

def process_missing_values():
    distinct_app_ids_user = get_distinct_app_ids()

    matching_records_dict = {}
    user_type_ids_set = set()
    user_type_count = {}
    batch_ids_info = {}
    distinct_logins_target = set()
    distinct_logins_from_user = set()
    missing_values_details = []
    missing_login_list = []
    formatted_indian_time = None

    for app_id in distinct_app_ids_user:
        matching_records = reconciliationPull.find({"applicationId": app_id})
        matching_records_dict[app_id] = [record for record in matching_records] 

        matching_records = reconciliationPull.find({"applicationId": app_id})
        user_type_ids_set.update(str(record.get('_id')) for record in matching_records if record.get('type') == 'USER')

        for user_type_id in user_type_ids_set:
            matching_sync_data = syncData.find({"reconciliationId": user_type_id})
            count = sum(1 for _ in matching_sync_data)
            user_type_count[str(user_type_id)] = count

        for user_type_id in user_type_ids_set:
            matching_sync_data = syncData.find({"reconciliationId": user_type_id})
            latest_created_datetime, latest_batch_id = None, None
            for sync_record in matching_sync_data:
                if latest_created_datetime is None or sync_record.get('createdDateTime') > latest_created_datetime:
                    latest_created_datetime = sync_record.get('createdDateTime')
                    latest_batch_id = sync_record.get('batchId')
            batch_ids_info[str(user_type_id)] = {'latest_batch_id': latest_batch_id, 'latest_created_datetime': latest_created_datetime}

        for user_type_id, info in batch_ids_info.items():
            latest_batch_id = info['latest_batch_id']
            matching_records = syncData.find({"batchId": latest_batch_id}, {"data.login": 1})
            distinct_logins_target.update(record.get("data", {}).get("login") for record in matching_records)

        for app_id in distinct_app_ids_user:
            matching_records = user_collection.find({"provisionedApps.{}.login.login".format(app_id): {"$exists": True}}, {"provisionedApps.{}.login.login".format(app_id): 1})
            distinct_logins_from_user.update(record.get("provisionedApps", {}).get(app_id, {}).get("login", {}).get("login") for record in matching_records)

    missing_values = distinct_logins_target - distinct_logins_from_user

    inserted_records_count = 0
    inserted_object_ids = []

    for missing_login in missing_values:
        matching_records_sync_data = syncData.find({"data.login": missing_login})
        for sync_record in matching_records_sync_data:
            sync_data_details = {
                "batchId": sync_record.get("batchId"),
                "reconciliationId": sync_record.get("reconciliationId"),
                "loginId": missing_login,
                "breakType": "Account created outside Cymmetri",
                "performedAt": datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + "+00:00"
            }
            matching_record_reconciliation_pull = reconciliationPull.find_one({"_id": ObjectId(sync_record.get("reconciliationId"))})
            if matching_record_reconciliation_pull:
                sync_data_details["applicationId"] = matching_record_reconciliation_pull.get("applicationId")
                missing_values_details.append(sync_data_details)

        for record in missing_values_details:
            criteria = {
                "breakType": record["breakType"],
                "loginId": record["loginId"],
                "applicationId": record["applicationId"],
                "reconciliationId": record["reconciliationId"]
            }

            update_operation = {
                "$set": {"performedAt": record["performedAt"]}
            }

            result = recon_break_type.update_one(criteria, update_operation, upsert=True)

            if result.matched_count == 0:
                inserted_records_count += 1
                inserted_object_ids.append(result.upserted_id)

        indian_time = get_indian_time()
        formatted_indian_time = indian_time.strftime('%Y-%m-%d %H:%M:%S %Z%z')

    break_count_document = {
        "status": f"{inserted_records_count} new records inserted",
        "performedAt": formatted_indian_time,
        "objectIDs": inserted_object_ids
    }
    break_count.insert_one(break_count_document)

def watch_recon_report_metadata():
    with recon_report_metadata.watch(full_document='updateLookup') as stream:
        for change in stream:
            print("Change detected in reconReportMetadata collection.")
            print(change)
            #process_missing_values()
            print("change is detected and function has run")

# FastAPI endpoint (just for example)
# @app.get("/")
# async def read_root():
#     return {"message": "FastAPI is running."}

# # Start the MongoDB change stream in a background task
# import asyncio
# from fastapi import BackgroundTasks

# @app.on_event("startup")
# async def startup_event():
#     background_tasks = BackgroundTasks()
#     background_tasks.add_task(watch_recon_report_metadata)

# if __name__ == "__main__":
#     # Start the FastAPI application
#     uvicorn.run(app, host="127.0.0.1", port=8000)
            
if __name__ == "__main__":
    watch_recon_report_metadata()
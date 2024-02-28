from fastapi import HTTPException
from pymongo import MongoClient
from bson import ObjectId
from datetime import datetime
import logging
from pytz import timezone

# MongoDB connection
client = MongoClient("mongodb://unoadmin:devDb123@10.0.1.6:27019,10.0.1.6:27020,10.0.1.6:27021/?authSource=admin&replicaSet=sso-rs&retryWrites=true&w=majority")
db = client['newqa-recontestsahil'] 
user_collection = db["user"]
reconciliationPull = db["reconciliationPull"]
syncData = db["syncData"]
break_count = db["break_count"]
recon_break_type = db["recon_break_type"]

def get_indian_time():
    tz = timezone('Asia/Kolkata')
    return datetime.now(tz)

def fetch_distinct_app_ids():
    pipeline = [
        {"$project": {"provisionedApps": {"$objectToArray": "$provisionedApps"}}},
        {"$unwind": "$provisionedApps"},
        {"$match": {"provisionedApps.k": {"$ne": "CYMMETRI"}}},
        {"$group": {"_id": "$provisionedApps.k"}}
    ]
    distinct_apps = user_collection.aggregate(pipeline)
    return [app['_id'] for app in distinct_apps]

def fetch_matching_records(app_id):
    matching_records = reconciliationPull.find({"applicationId": app_id})
    return [record for record in matching_records]

def fetch_user_type_ids(matching_records):
    user_type_ids = set()
    for record in matching_records:
        if record.get('type') == 'USER':
            user_type_ids.add(str(record.get('_id')))
    return user_type_ids

def fetch_sync_data_counts(user_type_ids):
    user_type_count = {}
    for user_type_id in user_type_ids:
        matching_sync_data = syncData.find({"reconciliationId": user_type_id})
        count = sum(1 for _ in matching_sync_data)
        user_type_count[str(user_type_id)] = count
    return user_type_count

def fetch_latest_batch_ids(user_type_ids):
    batch_ids_info = {}
    for user_type_id in user_type_ids:
        latest_batch_id = None
        latest_created_datetime = None
        matching_sync_data = syncData.find({"reconciliationId": user_type_id})
        for sync_record in matching_sync_data:
            if latest_created_datetime is None or sync_record.get('createdDateTime') > latest_created_datetime:
                latest_created_datetime = sync_record.get('createdDateTime')
                latest_batch_id = sync_record.get('batchId')
        batch_ids_info[str(user_type_id)] = {'latest_batch_id': latest_batch_id, 'latest_created_datetime': latest_created_datetime}
    return batch_ids_info

def fetch_distinct_logins_target(batch_ids_info):
    distinct_logins_target = set()
    for info in batch_ids_info.values():
        latest_batch_id = info['latest_batch_id']
        matching_records = syncData.find({"batchId": latest_batch_id}, {"data.login": 1})
        distinct_logins_target.update(record.get("data", {}).get("login") for record in matching_records)
    return distinct_logins_target

def fetch_distinct_logins_from_user(distinct_app_ids):
    distinct_logins_from_user = set()
    for app_id in distinct_app_ids:
        matching_records = user_collection.find({"provisionedApps.{}.login.login".format(app_id): {"$exists": True}}, {"provisionedApps.{}.login.login".format(app_id): 1})
        distinct_logins_from_user.update(record.get("provisionedApps", {}).get(app_id, {}).get("login", {}).get("login") for record in matching_records)
    return distinct_logins_from_user

def process_missing_values(missing_values):
    missing_values_details = []
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
    return missing_values_details

def update_break_type_collection(missing_values_details):
    inserted_records_count = 0
    inserted_object_ids = []
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
    return inserted_records_count, inserted_object_ids

def insert_break_count_document(inserted_records_count, inserted_object_ids):
    indian_time = get_indian_time()
    formatted_indian_time = indian_time.strftime('%Y-%m-%d %H:%M:%S %Z%z')
    break_count_document = {
        "status": f"{inserted_records_count} new records inserted",
        "performedAt": formatted_indian_time,
        "objectIDs": inserted_object_ids
    }
    break_count.insert_one(break_count_document)

try:
    distinct_app_ids_user = fetch_distinct_app_ids()
    matching_records_dict = {}
    user_type_ids_set = set()
    user_type_count = {}
    batch_ids_info = {}
    distinct_logins_target = set()
    distinct_logins_from_user = set()

    for app_id in distinct_app_ids_user:
        matching_records = fetch_matching_records(app_id)
        matching_records_dict[app_id] = matching_records
        user_type_ids_set.update(fetch_user_type_ids(matching_records))

    user_type_count = fetch_sync_data_counts(user_type_ids_set)
    batch_ids_info = fetch_latest_batch_ids(user_type_ids_set)
    distinct_logins_target = fetch_distinct_logins_target(batch_ids_info)
    distinct_logins_from_user = fetch_distinct_logins_from_user(distinct_app_ids_user)

    missing_values = distinct_logins_target - distinct_logins_from_user
    missing_values_details = process_missing_values(missing_values)
    inserted_records_count, inserted_object_ids = update_break_type_collection(missing_values_details)
    insert_break_count_document(inserted_records_count, inserted_object_ids)

except Exception as e:
    logging.error(f"Error occurred: {e}")
    raise HTTPException(status_code=500, detail=str(e))

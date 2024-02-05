from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from pymongo import MongoClient
from bson import ObjectId
from datetime import datetime
import uvicorn
from fastapi import FastAPI, Form, Request, HTTPException, Header,Query
from typing import Optional
import logging
from database.connection import get_collection
from pydantic import BaseModel

app = FastAPI()

class Response(BaseModel):
    code:int
    data: dict
    success: bool
    message: Optional[str] = None
    
live_count = 0

def __user_collection__(tenant: str):
    #logging.debug(f"Getting collection for tenant: {tenant}")
    return get_collection(tenant, "user")

def __reconciliationPull_collection__(tenant: str):
    #logging.debug(f"Getting collection for tenant: {tenant}")
    return get_collection(tenant, "reconciliationPull")

def __sync_collection__(tenant: str):
    #logging.debug(f"Getting collection for tenant: {tenant}")
    return get_collection(tenant, "syncData")

def __breaktype_collection__(tenant: str):
    #logging.debug(f"Getting collection for tenant: {tenant}")
    return get_collection(tenant, "recon_break_type")


from config.loader import Configuration
import platform
 
def load_configuration():
    if platform.system() == 'Windows':
        c = Configuration(r"C:/Users/pace it/Desktop/cymmetri/cymmetri-microservices-recon/config.yaml")
    else:
        c = Configuration("/config/config.yaml")
    return c.getConfiguration()
 
data = load_configuration()

base_prefix = data['BASE_PREFIX']

@app.get(f"{base_prefix}/health")
async def get_health_status():
    response_data = {
        "health":"running",
        "updated_ts":datetime.now()
    }
    return Response(code=200,data=response_data,success=True)

@app.get(f"{base_prefix}/break_1")
async def get_distinct_logins(tenant:str = Header(...)) -> Response:

    user_collection = __user_collection__(tenant)
    reconciliationPull = __reconciliationPull_collection__(tenant)
    syncData = __sync_collection__(tenant)

    # Your existing code
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

    try:
        distinct_apps_user = user_collection.aggregate(pipeline)
        distinct_app_ids_user = [app['_id'] for app in distinct_apps_user]

        if "CYMMETRI" in distinct_app_ids_user:
            distinct_app_ids_user.remove("CYMMETRI")

        matching_records_dict = {}
        for app_id in distinct_app_ids_user:
            matching_records = reconciliationPull.find({"applicationId": app_id})
            matching_records_dict[app_id] = [record for record in matching_records]

        user_type_ids_set = set()
        for app_id in distinct_app_ids_user:
            matching_records = reconciliationPull.find({"applicationId": app_id})
            user_type_ids_set.update(str(record.get('_id')) for record in matching_records if record.get('type') == 'USER')

        user_type_count = {}
        for user_type_id in user_type_ids_set:

            matching_sync_data = syncData.find({"reconciliationId": user_type_id})
            count = sum(1 for _ in matching_sync_data)
            user_type_count[str(user_type_id)] = count

        batch_ids_info = {}
        for user_type_id in user_type_ids_set:
            matching_sync_data = syncData.find({"reconciliationId": user_type_id})
            latest_created_datetime, latest_batch_id = None, None
            for sync_record in matching_sync_data:
                if latest_created_datetime is None or sync_record.get('createdDateTime') > latest_created_datetime:
                    latest_created_datetime = sync_record.get('createdDateTime')
                    latest_batch_id = sync_record.get('batchId')
            batch_ids_info[str(user_type_id)] = {'latest_batch_id': latest_batch_id, 'latest_created_datetime': latest_created_datetime}

        distinct_logins_target = set()
        for user_type_id, info in batch_ids_info.items():
            latest_batch_id = info['latest_batch_id']
            matching_records = syncData.find({"batchId": latest_batch_id}, {"data.login": 1})
            distinct_logins_target.update(record.get("data", {}).get("login") for record in matching_records)

        distinct_logins_from_user = set()
        for app_id in distinct_app_ids_user:
            matching_records = user_collection.find({"provisionedApps.{}.login.login".format(app_id): {"$exists": True}}, {"provisionedApps.{}.login.login".format(app_id): 1})
            distinct_logins_from_user.update(record.get("provisionedApps", {}).get(app_id, {}).get("login", {}).get("login") for record in matching_records)

        missing_values = distinct_logins_target - distinct_logins_from_user

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

        response_data = {
            # "distinct_app_ids_user": distinct_app_ids_user,
            # "user_type_ids_set": list(user_type_ids_set),
            # "user_type_count": user_type_count,
            # "batch_ids_info": batch_ids_info,
            # "distinct_logins_target": list(distinct_logins_target),
            # "distinct_logins_from_user": list(distinct_logins_from_user),
            # "missing_values": list(missing_values),
            "missing_values_details": missing_values_details
        }


    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
    print("response data: ",response_data)
    
    breaktype_collection = __breaktype_collection__(tenant)
    print("collection: ", breaktype_collection)

    for record in response_data["missing_values_details"]:
        print("Record:", record)

        # Define the criteria for finding duplicates
        criteria = {
            "Break_Type": record["breakType"],
            "loginId": record["loginId"],
            "applicationId": record["applicationId"],
            "reconciliationId": record["reconciliationId"]

        }

        # Define the update operation
        update_operation = {
            "$set": {"performedAt": record["performedAt"]}
        }

        # Try to update the existing document; if it doesn't exist, insert a new one
        result = breaktype_collection.update_one(criteria, update_operation, upsert=True)

        # Check if the update was successful
        if result.matched_count > 0:
            logging.debug(f"Record updated successfully: {criteria}")
        else:
            logging.debug(f"Record inserted successfully: {record}")

    #logging.debug(f"Data for login {record['user_login']} saved successfully")


    return Response(code=200,data={},success=True)


# @app.get("/status_check")
# async def status_check(tenant: str = Header(...)):
#     global live_count
#     try:
#         return {"records_inserted": live_count}
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))


    
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=5000)
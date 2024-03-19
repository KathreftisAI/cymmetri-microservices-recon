from bson import ObjectId
from pymongo import MongoClient
import datetime
import redis
from config.loader import Configuration
import platform
import logging

# MongoDB client initialization
client = MongoClient("mongodb://unoadmin:devDb123@10.0.1.6:27019,10.0.1.6:27020,10.0.1.6:27021/?authSource=admin&replicaSet=sso-rs&retryWrites=true&w=majority")
db = client["cymmetri-datascience"]
reconReportMetadata = db["reconReportMetadata"]
reconciliationPull = db["reconciliationPull"]
applicationTenant = db["applicationTenant"]
userCollection = db["user"]

# Global variables
current_utc_time = datetime.datetime.utcnow()
redis_client = None

# Function to get Redis connection
def get_redis_connection():
    global redis_client
    if redis_client is None:
        if platform.system() == 'Windows':
            config_path = "config.yaml"
        else:
            config_path = "/config/config.yaml"
        c = Configuration(config_path)
        data = c.getConfiguration()

        host = data["REDIS_HOST"]
        port = data["REDIS_PORT"]
        username = data["REDIS_USERNAME"]
        password = data["REDIS_PASSWORD"]
        database = data["REDIS_DATABASE"]

        conn_pool = redis.ConnectionPool(host=host, port=port, db=database, username=username, password=password)
        redis_client = redis.Redis(connection_pool=conn_pool)

    return redis_client

# Function to process a new document
def process_new_document(document):
    # Redis connection
    redis_conn = get_redis_connection()

    # Extracting _id, recon_id, and batch_id
    id_value = str(document["_id"])
    recon_id_value = document["reconciliationId"]
    batch_id_value = document["batchId"]

    # Printing values
    print("reconReportMetadata_id:", id_value)
    print("recon_id:", recon_id_value)
    print("batch_id:", batch_id_value)

    # Fetching applicationId from reconciliationPull collection
    reconciliation_doc = reconciliationPull.find_one({"_id": ObjectId(recon_id_value)})
    if reconciliation_doc:
        application_id = reconciliation_doc.get("applicationId")
        print("applicationId:", application_id)

        # Fetching displayName from applicationTenant collection
        application_tenant_doc = applicationTenant.find_one({"_id": ObjectId(application_id)})
        if application_tenant_doc:
            display_name = application_tenant_doc.get("settings", {}).get("displayName")
            print("ApplicationName", display_name)

            # Fetching logins from user collection
            logins = []
            users = []
            app_overdue = []
            final_app_overdue = []
            user_docs = userCollection.find({"provisionedApps.{}.login.login".format(application_id): {"$exists": True}})
            for user_doc in user_docs:
                login = user_doc["provisionedApps"][application_id]["login"]["login"]
                logins.append(login)
                users.append(user_doc)
            
            for user in users:
                # Check if user["provisionedApps"] is a dictionary
                if isinstance(user["provisionedApps"], dict):
                    # Check if applicationId exists in provisionedApps
                    if application_id in user["provisionedApps"]:
                        # Check the conditions for the user
                        if "end_date" in user and user["end_date"] < current_utc_time and \
                        user["status"] == "ACTIVE" and \
                        user["provisionedApps"][application_id]["login"]["status"] not in ["SUCCESS_DELETE", "SUCCESS_UPDATE"]:
                                app_overdue.append(user)
                        else:
                            print("No App_Overdue Found")
            for user in app_overdue:
                # Extracting desired attributes from each user
                user_data = {
                    "cymmetriLogin": user["provisionedApps"]["CYMMETRI"]["login"]["login"],
                    "appLogin": user["provisionedApps"][application_id]["login"]["login"],
                    "status": user["status"],
                    "appStatus": user["provisionedApps"][application_id]["login"]["status"],
                    "end_date": user.get("end_date", None)
                }
                final_app_overdue.append(user_data)
            print("Cymmetri_logins:", logins)
            print("users:", users)
            print(len(users))
            print("app_overdue", app_overdue)
            print(len(app_overdue))
            print("final_app_overdue:", final_app_overdue)

            # Store data in Redis
            success = store_in_redis(id_value, display_name, application_id, logins, final_app_overdue)
            if success:
                print("Data inserted into Redis successfully!")
            else:
                print("Failed to insert data into Redis.")
            
        else:
            print("Application not found in applicationTenant collection")
    else:
        print("reconciliationId not found in reconciliationPull collection")

# Function to store data in Redis
def store_in_redis(recon_report_metadata_id, app_name, app_id, logins, final_app_overdue):
    try:
        # Redis connection
        redis_conn = get_redis_connection()

        # Static Redis key
        redis_key = "cymmetri-datascience_sh_recon_65f16ea71eea4028c1fa4c32_f595e9a3-77dd-476a-a424-3336552e3bac"

        # Convert end_date to string representation in final_app_overdue
        for user_data in final_app_overdue:
            if "end_date" in user_data and user_data["end_date"] is not None:
                user_data["end_date"] = user_data["end_date"].isoformat()

        # Storing data in Redis
        redis_conn.hset(redis_key, "reconReportMetadataId", recon_report_metadata_id)
        redis_conn.hset(redis_key, "appName", app_name)
        redis_conn.hset(redis_key, "appId", app_id)
        redis_conn.hset(redis_key, "Cymmetri_logins", str(logins))
        redis_conn.hset(redis_key, "final_app_overdue", str(final_app_overdue))

        return True
    except Exception as e:
        print("Error inserting data into Redis:", str(e))
        return False


# Watch the collection for changes
with reconReportMetadata.watch(full_document='updateLookup') as stream:
    for change in stream:
        if change["operationType"] == "insert":
            new_document = change["fullDocument"]
            process_new_document(new_document)

from pymongo import MongoClient
from datetime import datetime

# Connect to MongoDB
client = MongoClient("mongodb://unoadmin:devDb123@10.0.1.6:27019,10.0.1.6:27020,10.0.1.6:27021/?authSource=admin&replicaSet=sso-rs&retryWrites=true&w=majority")
db = client["newqa-recontestsahil"]

# Function to check Cymmetri overdue
def check_cymmetri_overdue():
    current_utc_time = datetime.utcnow()
    users = db.user.find({
        "end_date": {"$exists": True, "$lt": current_utc_time},
        "status": "ACTIVE"
    })
    users_list = list(users) 
    users_count = len(users_list)
    print("Cymmetri Users Count:", users_count)

    # Print details of each matching document
    for user in users_list:
        user_id = str(user["_id"])
        display_name = user.get("displayName", "N/A")
        end_date = user.get("end_date", "N/A")
        current_date = current_utc_time

        print(f"User ID: {user_id}, Display Name: {display_name}, End Date: {end_date}, Current Date: {current_date}")
    

# # # Function to check App overdue
# # def check_app_overdue():
# #     users = db.user.find({
# #         "end_date": {"$exists": True, "$lt": datetime.utcnow()}
# #     })
    
# #     # Printing actual data
# #     for user in users:
# #         print("User:", user)

# #     # Printing count of users
# #     users_count = db.user.count_documents({
# #         "end_date": {"$exists": True, "$lt": datetime.utcnow()}
# #     })
# #     print("App Users Count:", users_count)

# # Call the function
# #check_app_overdue()
    
# # Check Cymmetri overdue
# check_cymmetri_overdue()




def check_app_overdue():
    current_utc_time = datetime.utcnow()

    # Find users with end_date less than today's date
    users = db.user.find({
        "end_date": {"$exists": True, "$lt": current_utc_time},
        "status": "ACTIVE"
    })

    users_list = list(users)

    

    # Iterate over users
    for user in users_list:
        break_type = print("Break Type:: App_Ovedue")
        user_id = str(user["_id"])
        display_name = user.get("displayName", "N/A")
        end_date = user.get("end_date", "N/A")

        # Iterate over provisionedApps for each user
        for app_id, app_details in user.get("provisionedApps", {}).items():
            app_login = app_details.get("login", {}).get("login", "N/A")
            app_status = app_details.get("login", {}).get("status", "N/A")

            # Check if app_status is not "SUCCESS_DELETE"
            #Include "SUCCESS_UPDATE"
            if app_status != "SUCCESS_DELETE" or "SUCCESS_UPDATE":
                print(f"User ID: {user_id}, Display Name: {display_name}, "
                      f"App ID: {app_id}, App Status: {app_status}, "
                      f"Cymmetri Status: {user['status']}, "
                      f"End Date: {end_date}, Current Date: {current_utc_time}")

# Call the function
check_cymmetri_overdue()
print("================================================================================================")
check_app_overdue()



from pymongo import MongoClient
from datetime import datetime

# Connect to MongoDB
client = MongoClient("mongodb://unoadmin:devDb123@10.0.1.6:27019,10.0.1.6:27020,10.0.1.6:27021/?authSource=admin&replicaSet=sso-rs&retryWrites=true&w=majority")
db = client["cymmetri-datascience"]



# Function to check Cymmetri overdue
# def check_cymmetri_overdue():
#     current_utc_time = datetime.utcnow()
#     users = db.user.find({
#         "end_date": {"$exists": True, "$lt": current_utc_time},
#         "status": "ACTIVE"
#     })
#     users_list = list(users) 
#     users_count = len(users_list)
#     print("Cymmetri Users Count:", users_count)

#     # Print details of each matching document
#     for user in users_list:
#         user_id = str(user["_id"])
#         display_name = user.get("displayName", "N/A")
#         end_date = user.get("end_date", "N/A")
#         current_date = current_utc_time

#         print(f"User ID: {user_id}, Display Name: {display_name}, End Date: {end_date}, Current Date: {current_date}")
    

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

app_ids = ['653f482a0d2e4b5f289727ff']
Cymmetri_Logins = ['sahil123', '74681']

def check_app_overdue():
    current_utc_time = datetime.utcnow()
    matching_users = []

    for app_id in app_ids:
        for cymmetri_login in Cymmetri_Logins:
            users = db.user.find({
                "end_date": {"$exists": True, "$lt": current_utc_time},
                "status": "ACTIVE",
                f"provisionedApps.{app_id}.login.login": cymmetri_login
                #"provisionedApps.CYMMETRI": {"$exists": False}  # Exclude records with App ID: CYMMETRI
            })
            matching_users.extend(users)
        print("matching user ",matching_users)
    for user in matching_users:
        break_type = print("Break Type:: App_Ovedue")
        user_id = str(user["_id"])
        display_name = user.get("displayName", "N/A")
        end_date = user.get("end_date", "N/A")
        for app_id, app_details in user.get("provisionedApps", {}).items():
            app_login = app_details.get("login", {}).get("login", "N/A")
            app_status = app_details.get("login", {}).get("status", "N/A")

            if app_status != "SUCCESS_DELETE" or "SUCCESS_UPDATE":
                print(f"User ID: {user_id}, Display Name: {display_name}, "
                      f"App ID: {app_id}, App Status: {app_status}, "
                      f"Cymmetri Status: {user['status']}, "
                      f"End Date: {end_date}, Current Date: {current_utc_time}")

    # # Remove duplicates from the list of dictionaries
    # seen = set()
    # unique_users = []
    # for user in matching_users:
    #     user_id = user["_id"]
    #     if user_id not in seen:
    #         seen.add(user_id)
    #         unique_users.append(user)

    # for user in unique_users:
    #     break_type = print("Break Type:: App_Ovedue")
    #     user_id = str(user["_id"])
    #     display_name = user.get("displayName", "N/A")
    #     end_date = user.get("end_date", "N/A")

    #     for app_id, app_details in user.get("provisionedApps", {}).items():
    #         app_login = app_details.get("login", {}).get("login", "N/A")
    #         app_status = app_details.get("login", {}).get("status", "N/A")

    #         if app_status != "SUCCESS_DELETE" or "SUCCESS_UPDATE":
    #             print(f"User ID: {user_id}, Display Name: {display_name}, "
    #                   f"App ID: {app_id}, App Status: {app_status}, "
    #                   f"Cymmetri Status: {user['status']}, "
    #                   f"End Date: {end_date}, Current Date: {current_utc_time}")





# Call the function
#check_cymmetri_overdue()
print("================================================================================================")
check_app_overdue()



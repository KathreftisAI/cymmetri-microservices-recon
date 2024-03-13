from pymongo import MongoClient
import time

# Connect to MongoDB
client = MongoClient("mongodb://unoadmin:devDb123@10.0.1.6:27019,10.0.1.6:27020,10.0.1.6:27021/?authSource=admin&replicaSet=sso-rs&retryWrites=true&w=majority")
db = client['cymmetri-datascience']  # Replace 'cymmetri-datasceice' with your actual database name

def fetch_recon_report_metadata():
    # Fetch the latest record from reconReportMetadata collection
    recon_metadata = db.reconReportMetadata.find_one({}, sort=[('_id', -1)])
    return recon_metadata

def fetch_latest_break_metadata():
    # Fetch the latest record from breakReportMetadata collection
    break_metadata = db.breakReportMetadata.find_one({}, sort=[('_id', -1)])
    return break_metadata

def update_dashboard_data(recon_metadata, break_metadata):
    # Update dashboardData collection with latest recon metadata and break metadata
    latest_recon_data = {
        'latestReconRun': recon_metadata['createdDateTime'],
        'totlaReord': recon_metadata['recordCount'],
        'breakCount': break_metadata['status'],  # Include status from breakReportMetadata
        'breakType': break_metadata['breakType']  # Include breakType from breakReportMetadata
    }
    # You can add more attributes from different collections here
    
    # Update the dashboardData collection with the latest recon data
    db.dashBoardData.update_one({}, {'$set': {'latestReconRunData': latest_recon_data}}, upsert=True)
    print(latest_recon_data)

if __name__ == "__main__":
    while True:
        # Check for new records in reconReportMetadata collection
        recon_metadata = fetch_recon_report_metadata()
        if recon_metadata:
            # Fetch the latest break metadata
            break_metadata = fetch_latest_break_metadata()
            if break_metadata:
                update_dashboard_data(recon_metadata, break_metadata)
                print("Updated dashBoardData with latest recon data.")
            else:
                print("No latest break metadata found.")
        else:
            print("No latest recon metadata found.")
        
        # Sleep for 30 seconds before checking again
        time.sleep(30)

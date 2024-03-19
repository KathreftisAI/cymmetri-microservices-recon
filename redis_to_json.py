import json
import redis
import logging


# Redis configurations
REDIS_HOST = '10.0.1.7'
REDIS_PORT = 6379
REDIS_USERNAME = 'infra'
REDIS_PASSWORD = 'infra@123'
REDIS_DATABASE = 1

# Key to fetch data for
key = 'cymmetri-datascience_sh_recon_65f16ea71eea4028c1fa4c32_f595e9a3-77dd-476a-a424-3336552e3bac'

# Connect to Redis
redis_client = redis.StrictRedis(host=REDIS_HOST,
                                 port=REDIS_PORT,
                                 username=REDIS_USERNAME,
                                 password=REDIS_PASSWORD,
                                 db=REDIS_DATABASE,
                                 decode_responses=True)

# Fetch data for the key
data = redis_client.hgetall(key)
print(type(data))

# Check if data is found
if data is not None:
    print("Data for key '{}' found: {}".format(key, data))
else:
    print("No data found for key '{}'".format(key))



import json

# Assuming 'data' contains the fetched data from Redis
cymmetri_logins_dict = json.loads(data['Cymmetri_logins'])
final_app_overdue_dict = json.loads(data['final_app_overdue'])

# Create a new dictionary for Final App Overdue in the desired format
cymmetri_logins_formatted = {"Cymmetri Logins":cymmetri_logins_dict}
final_app_overdue_formatted = {'Final App Overdue': final_app_overdue_dict}

print(type(cymmetri_logins_dict))
print(type(final_app_overdue_dict))





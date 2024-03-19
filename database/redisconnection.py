import redis
from config.loader import Configuration
import platform
import logging

global connectionTenant
connectionTenant = {}

def getRedisConnectionFromPool():
    if "default" in connectionTenant.keys():
        logging.debug(f"Connection being fetched directly from previous connection pool")
        return connectionTenant["default"]

    if platform.system() == 'Windows':
        c = Configuration("C:\\Users\\Admin\\source\\repos\\cymmetri-microservices-adaptive-ml\\config.yaml")
    else:
        c = Configuration("/config/config.yaml")
    data = c.getConfiguration()

        
    host = data["REDIS_HOST"]
    port = data["REDIS_PORT"]
    username = data["REDIS_USERNAME"]
    password = data["REDIS_PASSWORD"]
    database = data["REDIS_DATABASE"]
    conn_pool = redis.ConnectionPool(host=host,port=port,db=database,username=username,password=password)
    r = redis.Redis(connection_pool=conn_pool)
    connectionTenant["default"] = r
    logging.debug(f"connection tenant {connectionTenant}")
        
    return r

    
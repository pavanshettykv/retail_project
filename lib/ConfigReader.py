import configparser
from pyspark import SparkConf
# loading the application configs in python dictionary
def get_app_config(env):
    config = configparser.ConfigParser()
    config.read("configs/application.conf")
    app_conf = {}
    for (key, val) in config.items(env):
        app_conf[key] = val
    return app_conf

# loading the pyspark configs and creating a spark conf object
def get_pyspark_config(env):
    config = configparser.ConfigParser()
    config.read("configs/pyspark.conf")
    # print(config)
    pyspark_conf = SparkConf()
    for (key, val) in config.items(env):
        pyspark_conf.set(key, val)
        # print(pyspark_conf)
    return pyspark_conf


# conf = get_pyspark_config('LOCAL')
# print(f'{1} conf')
# orders= "data/orders.csv"
# # orders_file_path = conf["orders"]

# print(orders_file_path)
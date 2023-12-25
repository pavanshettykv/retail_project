import pytest

from lib.DataReader import read_customers,read_orders
from lib.DataManipulation import filter_closed_orders,count_orders_state
from lib.ConfigReader import get_app_config

def test_read_customers(spark):
    customers_count = read_customers(spark,"LOCAL").count()
    assert customers_count == 12434

def test_orders_customers(spark):
    orders_count = read_orders(spark,"LOCAL").count()
    assert orders_count == 68882
    
@pytest.mark.transformation() # marking as transformation
def test_filter_closed_orders(spark):
    orders_df = read_orders(spark,"LOCAL")
    order_filtered_count = filter_closed_orders(orders_df).count()
    assert order_filtered_count == 7555

def test_get_app_config():
    conf = get_app_config("LOCAL")
    assert conf['customers.file.path'] == 'data/customers.csv'

@pytest.mark.transformation() # marking as transformation
def test_count_orders_state(spark,expected_df):
    customers_df = read_customers(spark,"LOCAL")
    acutal_df= count_orders_state(customers_df)
    assert acutal_df.collect() == expected_df.collect()
       
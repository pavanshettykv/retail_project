import pytest

from lib.DataReader import read_customers,read_orders
from lib.DataManipulation import filter_closed_orders,count_orders_state,filter_generic_orders
from lib.ConfigReader import get_app_config

@pytest.mark.skip
def test_read_customers(spark):
    customers_count = read_customers(spark,"LOCAL").count()
    assert customers_count == 12434

@pytest.mark.skip
def test_orders_customers(spark):
    orders_count = read_orders(spark,"LOCAL").count()
    assert orders_count == 68882
    
@pytest.mark.skip# marking as transformation
def test_filter_closed_orders(spark):
    orders_df = read_orders(spark,"LOCAL")
    order_filtered_count = filter_closed_orders(orders_df).count()
    assert order_filtered_count == 7555

@pytest.mark.skip
def test_get_app_config():
    conf = get_app_config("LOCAL")
    assert conf['customers.file.path'] == 'data/customers.csv'

@pytest.mark.skip # marking as transformation
def test_count_orders_state(spark,expected_df):
    customers_df = read_customers(spark,"LOCAL")
    acutal_df= count_orders_state(customers_df)
    assert acutal_df.collect() == expected_df.collect()

@pytest.mark.skip
def test_filter_generic_orders_closed(spark):
    orders_df = read_orders(spark,"LOCAL")
    order_filtered_count = filter_generic_orders(orders_df,"CLOSED").count()
    assert order_filtered_count == 7555

@pytest.mark.skip
def test_filter_generic_orders__pending(spark):
    orders_df = read_orders(spark,"LOCAL")
    order_filtered_count = filter_generic_orders(orders_df,"PENDING_PAYMENT").count()
    assert order_filtered_count == 15030

@pytest.mark.skip
def test_filter_generic_orders_complete(spark):
    orders_df = read_orders(spark,"LOCAL")
    order_filtered_count = filter_generic_orders(orders_df,"COMPLETE").count()
    assert order_filtered_count == 22899

#parameterized
    
@pytest.mark.parametrize(
        "status,count",
        [("CLOSED",7555),
         ("PENDING_PAYMENT",15030),
         ("COMPLETE",22899)
         ]
)

def test_check_count(spark,status,count):
    orders_df = read_orders(spark,"LOCAL")
    order_filtered_count = filter_generic_orders(orders_df,status).count()
    assert order_filtered_count == count
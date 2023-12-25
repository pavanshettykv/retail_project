import pytest
from lib.Utils import get_spark_session

@pytest.fixture
def spark():
    "creates spark session"
    spark_session =  get_spark_session("LOCAL")
    yield spark_session
    spark_session.stop()


# def spark():
#     return get_spark_session("LOCAL")
    
@pytest.fixture
def expected_df():
    "returns expected results"
    spark = get_spark_session("LOCAL")
    result_schema = "state string,count int"
    return spark.read\
        .format("csv")\
        .schema(result_schema)\
        .option("header",True)\
        .load("data/test_result/state_agg.csv")
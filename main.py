from pathlib import Path
from typing import List

from pyspark.sql import SparkSession
import pyspark.sql.functions as f

from data_request.cqc_location_data import get_location_specific_data, get_location_data_as_json


def get_json_data_as_dataframe():
    path = Path('./data/data.json')
    print(path.absolute())

    multiline_df = spark.read.option("multiline", "true") \
        .json('./data/data.json')

    return multiline_df


def get_location_data_as_dataframe():
    multiline_df = spark.read \
        .json('./data/location_data.json')

    return multiline_df


# take 3 Ids, request data from the api and join that to our table
# join on locationid nad select region, constituency and localAuthority

# RDD = resilient distributed dataset
def get_list_of_location_ids(df: f.DataFrame) -> List[str]:
    location_list = df.select(df.locationId).rdd.flatMap(lambda x: x).collect()
    return location_list


# Request: https://api.cqc.org.uk/public/v1/locations/1-545611283?partnerCode=OpenAnswers

if __name__ == "__main__":
    spark = SparkSession.builder.appName("spark_sandbox").getOrCreate()
    json_df = get_json_data_as_dataframe()
    cleaned_up_df = json_df.withColumn('LocationId_trimmed', f.trim(f.col('locationId')))
    df_small = cleaned_up_df.withColumn('index', f.monotonically_increasing_id())
    df_ten_vals = df_small.filter(f.col('index') < 10).drop(f.col('index'))
    location_list = get_list_of_location_ids(df_ten_vals)

    # take 3 Ids, request data from the api and join that to our table
    # join on locationid nad select region, constituency and localAuthority
    # get_location_specific_data(location_list)

    location_data_df = get_location_data_as_dataframe().select(f.col('region'), f.col('constituency'),
                                                               f.col('localAuthority'), f.col('locationId'))
    location_data_df.show()

    location_and_area_df = location_data_df.join(df_ten_vals, location_data_df.locationId == df_ten_vals.locationId)\
        .drop(df_ten_vals.locationId).show()


    # postalCode
    # cleaned_up_df.filter(f.col('postalCode').like('NW%')).show()

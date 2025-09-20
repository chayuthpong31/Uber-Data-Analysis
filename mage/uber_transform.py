import pandas as pd
import requests
import io
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(df, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])

    df = df.drop_duplicates().reset_index(drop=True)
    df['trip_id'] = df.index

    # Create datetime_dim table
    datetime_dim = df[['tpep_pickup_datetime','tpep_dropoff_datetime']].drop_duplicates().reset_index(drop=True)
    datetime_dim['tpep_pickup_datetime'] = datetime_dim['tpep_pickup_datetime']
    datetime_dim['pick_hour'] = datetime_dim['tpep_pickup_datetime'].dt.hour
    datetime_dim['pick_day'] = datetime_dim['tpep_pickup_datetime'].dt.day
    datetime_dim['pick_month'] = datetime_dim['tpep_pickup_datetime'].dt.month
    datetime_dim['pick_year'] = datetime_dim['tpep_pickup_datetime'].dt.year
    datetime_dim['pick_weekday'] = datetime_dim['tpep_pickup_datetime'].dt.weekday

    datetime_dim['tpep_dropoff_datetime'] = datetime_dim['tpep_dropoff_datetime']
    datetime_dim['drop_hour'] = datetime_dim['tpep_dropoff_datetime'].dt.hour
    datetime_dim['drop_day'] = datetime_dim['tpep_dropoff_datetime'].dt.day
    datetime_dim['drop_month'] = datetime_dim['tpep_dropoff_datetime'].dt.month
    datetime_dim['drop_year'] = datetime_dim['tpep_dropoff_datetime'].dt.year
    datetime_dim['drop_weekday'] = datetime_dim['tpep_dropoff_datetime'].dt.weekday

    datetime_dim['datetime_id'] = datetime_dim.index

    datetime_dim = datetime_dim[[ 'datetime_id','tpep_pickup_datetime', 'tpep_dropoff_datetime', 'pick_hour',
        'pick_day', 'pick_month', 'pick_year', 'pick_weekday', 'drop_hour',
        'drop_day', 'drop_month', 'drop_year', 'drop_weekday']]

    # Crate passenger_dim table
    passenger_count_dim = df[['passenger_count']].drop_duplicates().reset_index(drop=True)
    passenger_count_dim['passenger_count_id'] = passenger_count_dim.index
    passenger_count_dim = passenger_count_dim[['passenger_count_id','passenger_count']]

    # Create trip_distance_dim table
    trip_distance_dim = df[['trip_distance']].drop_duplicates().reset_index(drop=True)
    trip_distance_dim['trip_distance_id'] = trip_distance_dim.index
    trip_distance_dim = trip_distance_dim[['trip_distance_id','trip_distance']]

    url = 'https://storage.googleapis.com/uber-data-engineering-project-chayuth/taxi_zone_lookup.csv'
    response = requests.get(url)

    taxi_zone = pd.read_csv(io.StringIO(response.text), sep=',')
    # Create drop_location_dim table
    drop_location_dim = df[['DOLocationID']].drop_duplicates()
    drop_location_dim = pd.merge(drop_location_dim, taxi_zone, left_on='DOLocationID', right_on='LocationID')
    drop_location_dim = drop_location_dim.rename(columns={'DOLocationID':'drop_location_id'})
    drop_location_dim = drop_location_dim.drop('LocationID',axis=1)

    # Create drop_location_dim table
    pick_location_dim = df[['PULocationID']].drop_duplicates()
    pick_location_dim = pd.merge(pick_location_dim, taxi_zone, left_on='PULocationID', right_on='LocationID')
    pick_location_dim = pick_location_dim.rename(columns={'PULocationID':'pick_location_id'})
    pick_location_dim = pick_location_dim.drop('LocationID',axis=1)

    # Create rate_code_dim table
    rate_code = {1:'Standard rate', 2:'JFK', 3:'Newark',4:'Nassau or Westchester', 5:'Negotiated fare',6:'Group ride', 99:'Null/unknown'}

    rate_code_dim = df[['RatecodeID']].drop_duplicates().reset_index(drop=True)
    rate_code_dim['rate_code_id'] = rate_code_dim.index
    rate_code_dim['Rate_code_name'] = rate_code_dim['RatecodeID'].map(rate_code)
    rate_code_dim = rate_code_dim[['rate_code_id','RatecodeID','Rate_code_name']]

    # Create payment_type_dim table
    payments = {
        1:"Credit card",
        2:"Cash",
        3:"No charge",
        4:"Dispute",
        5:"Unknown",
        6:"Voided trip"
    }
    payment_type_dim = df[['payment_type']].drop_duplicates().reset_index(drop=True)
    payment_type_dim['payment_type_id'] = payment_type_dim.index
    payment_type_dim['payment_type_name'] = payment_type_dim['payment_type'].map(payments)
    payment_type_dim = payment_type_dim[['payment_type_id','payment_type','payment_type_name']]

    # Create Fact table
    fact_table = df.merge(passenger_count_dim, on='passenger_count') \
                    .merge(trip_distance_dim, on='trip_distance') \
                    .merge(rate_code_dim, on='RatecodeID') \
                    .merge(payment_type_dim, on='payment_type') \
                    .merge(pick_location_dim,left_on='PULocationID', right_on='pick_location_id') \
                    .merge(drop_location_dim, left_on='DOLocationID',right_on='drop_location_id') \
                    .merge(datetime_dim, on=['tpep_pickup_datetime','tpep_dropoff_datetime']) \
                    [['trip_id','VendorID', 'datetime_id', 'passenger_count_id',
                'trip_distance_id', 'rate_code_id', 'store_and_fwd_flag', 'pick_location_id', 'drop_location_id',
                'payment_type_id', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
                'improvement_surcharge', 'total_amount']]

    return {"datetime_dim":datetime_dim.to_dict(orient="dict"),
    "passenger_count_dim":passenger_count_dim.to_dict(orient="dict"),
    "trip_distance_dim":trip_distance_dim.to_dict(orient="dict"),
    "rate_code_dim":rate_code_dim.to_dict(orient="dict"),
    "pickup_location_dim":pick_location_dim.to_dict(orient="dict"),
    "dropoff_location_dim":drop_location_dim.to_dict(orient="dict"),
    "payment_type_dim":payment_type_dim.to_dict(orient="dict"),
    "fact_table":fact_table.to_dict(orient="dict")}


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
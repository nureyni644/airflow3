# CONCEPT1: Assets
    # create assets
from airflow.adk import Asset,asset

assets = Asset(
    name="my_asset",
    uri="s3://my-bucket/data.csv",
    group="my_group"
)
# create asset using decorator
@asset(schedul="@daily",uri="s3://my-bucket/decorated_data.csv",group="decorated_group"
)
def my_asset():
    pass

# Concept2: Asset Events
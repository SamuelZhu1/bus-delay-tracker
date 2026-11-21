"""DynamoDB table definitions, shared by tests, local runs and docs (SAM template mirrors them)."""
from .config import TABLE_ARRIVALS, TABLE_ROUTE_STATS


def create_tables(ddb) -> None:
    existing = {t.name for t in ddb.tables.all()}
    if TABLE_ARRIVALS not in existing:
        ddb.create_table(
            TableName=TABLE_ARRIVALS,
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}, {"AttributeName": "sk", "KeyType": "RANGE"}],
            AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}, {"AttributeName": "sk", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        ).wait_until_exists()
    if TABLE_ROUTE_STATS not in existing:
        ddb.create_table(
            TableName=TABLE_ROUTE_STATS,
            KeySchema=[{"AttributeName": "route_id", "KeyType": "HASH"}, {"AttributeName": "hour", "KeyType": "RANGE"}],
            AttributeDefinitions=[{"AttributeName": "route_id", "AttributeType": "S"}, {"AttributeName": "hour", "AttributeType": "N"}],
            BillingMode="PAY_PER_REQUEST",
        ).wait_until_exists()

import json
import time
import signal
import sys


from google.api_core.exceptions import NotFound, DeadLineExceeded
from google.cloud import pubsub_v1
from google.cloud import bigquery


PROJECT_ID = "helpful-data-483403-d6"
SUBSCRIPTION_ID = "retail-data-test-sub"

BQ_DATASET = "retail_data_test"

MAX_MESSAGES = 1
PULL_TIMEOUT = 10
SLEEP_ON_EMPTY = 5

def tbl_verify_create(bq_client, table_id, msg_row1):
    
    schema = []

    for key, value in msg_row1.items():
        if isinstance(value, str):
            schema.append(bigquery.SchemaField(key, "STRING"))
        elif isinstance(value, int):
            schema.append(bigquery.SchemaField(key, "INTEGER"))
        elif isinstance(value, float):
            schema.append(bigquery.SchemaField(key, "FLOAT"))
        elif isinstance(value, bool):
            schema.append(bigquery.SchemaField(key, "BOOLEAN"))
        else:
            schema.append(bigquery.SchemaField(key, "STRING"))

    try:
        bq_client.get_table(table_id)
        print(f"BigQuery table exists: {table_id}")
    except NotFound:
        table = bigquery.Table(table_id, schema=schema)

        table = bq_client.create_table(table)
        print(f"Created BigQuery table: {table.full_table_id}")


def main():
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(
        PROJECT_ID, SUBSCRIPTION_ID
    )

    bq_client = bigquery.Client(project=PROJECT_ID)

    print("Starting batch pull job...")

    while True:
        try:
            response = subscriber.pull(
                request={
                    "subscription": subscription_path,
                    "max_messages": MAX_MESSAGES,
                },
                timeout=PULL_TIMEOUT,
            )
        except DeadLineExceeded:
            break

        for received_message in response.received_messages:
            ack_id = received_message.ack_id
            message = received_message.message

            try:
                payload = message.data.decode("utf-8")
                rows = json.loads(payload)

                if not isinstance(rows, list):
                    raise ValueError("Expected JSON array of rows")

                table_name = message.attributes["table"]
                table_id = f"{PROJECT_ID}.{BQ_DATASET}.{table_name}"
                tbl_verify_create(bq_client, table_id, rows[0])

                errors = bq_client.insert_rows_json(
                    table_id,
                    rows,
                )


                if errors:
                    raise RuntimeError(f"BigQuery insert errors: {errors}")

                subscriber.acknowledge(
                    request={
                        "subscription": subscription_path,
                        "ack_ids": [ack_id],
                    }
                )

                print(
                    f"Processed message {message.message_id} "
                    f"({len(rows)} rows)"
                )

            except Exception as e:
                # Do NOT ack — message will be retried
                print(
                    f"Failed processing message {message.message_id}: {e}"
                )


if __name__ == "__main__":
    main()

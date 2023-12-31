import pdb
import json
import json
import asyncio
import traceback
from settings import write_db_name,write_db_user,write_db_password,write_db_port,write_db_url,TOPIC,GROUP_ID,CLIENT_ID,KAFKA_BOOTSTRAP_SERVERS,TABLE_NAME,PRODUCER_CLIENT_ID,DLQ_PRODUCER_TOPIC
from aiokafka import AIOKafkaConsumer
from aiokafka.producer.producer import AIOKafkaProducer

import logging
from cg_database import Postgres
from datetime import datetime,timedelta
# -----------  CONSTANT  ------------

BASE_QUERY = """
INSERT INTO public.{}({})
VALUES ({})
ON CONFLICT (id) DO UPDATE
SET {};
"""
ist_offset = timedelta(minutes=30,hours=5)

global write_cursor
global write_db_connection


column_data_type_mapping = {
    'id': 'integer',
    'company_id': 'uuid',
    'loan_id': 'character varying',
    'notice_type': 'character varying',
    'document_type': 'character varying',
    'status': 'character varying',
    's3_link': 'character varying',
    's3_link_uuid': 'character varying',
    'data': 'jsonb',
    'updated': 'timestamp without time zone',
    'created': 'timestamp without time zone',
    'author': 'character varying',
    'role': 'character varying',
    'allocation_month': 'character varying',
    'is_closed': 'boolean',
    'stage_code': 'integer',
    'case_type': 'character varying',
    'is_in_case': 'boolean',
    'case_id': 'character varying',
    'iteration': 'integer',
    'archive': 'boolean',
    'is_linked_loan': 'boolean',
    'linked_loan_id': 'character varying',
    'local_pdf_file_name': 'character varying',
    'primary_address': 'text',
    'batch_id': 'character varying',
    'tracking_id': 'character varying',
    'notice_mode': 'character varying',
    'notice_id': 'character varying',
    'author_id': 'character varying',
    'is_dsc_signed': 'boolean',
    'dsc_placement': 'character varying',
    'old_s3_link': 'character varying',
    'old_s3_link_uuid': 'character varying',
    'is_deleted': 'boolean',
    'primary_notice_data': 'jsonb',
    'notice_row_id': 'integer',
    'notice_batch_id': 'character varying',
    'deleted': 'timestamp without time zone',
    'updated_by': 'character varying',
    'security_type': 'character varying',
    'security_id': 'character varying',
    'pod_link': 'character varying',
    'pod_batch_id': 'character varying'
}

int_columns = {k if v in ("integer",) else None for k,v in column_data_type_mapping.items()}
int_columns.remove(None)
boolean_columns = {k if v in ("boolean",) else None for k,v in column_data_type_mapping.items()}
boolean_columns.remove(None)
jsonb_columns = {k if v in ("jsonb",) else None for k,v in column_data_type_mapping.items()}
jsonb_columns.remove(None)
timestamp_columns = {k if v in ("timestamp without time zone",) else None for k,v in column_data_type_mapping.items()}
timestamp_columns.remove(None)

print(f"int_columns => {int_columns}")
print(f"boolean_columns => {boolean_columns}")
print(f"jsonb_columns => {jsonb_columns}")
print(f"timestamp_columns => {jsonb_columns}")

# special_handling_columns.add("created")
# special_handling_columns.add("updated")


#----------- DB CONNECTION---------------#

async def init_db():
    kwargs = {
        "database": write_db_name,
        "host": write_db_url,
        "port": write_db_port,
        "user": write_db_user,
        "password": write_db_password,
        "enable_read_replica": False,
        "read_replica_host": "",
        "read_replica_port": "",
        "publish_dashboard_events": False,
        "logging_handler": logging,
    }
    db = Postgres(**kwargs)
    await db.connect()
    return db


async def perform_upsert(db,msg,producer):
    print("perform_upsert")
    columns = []
    values = []
    query = ""
    try:
        for column,value in msg.items():

            if value == "__debezium_unavailable_value" or column in ("audit_timestamp","audit_operation","__deleted"):
                print(f"perform_upsert.__debezium_unavailable_value for id - {msg.get('id')} for column - {column}")
                continue

            if column == "role":
                columns.append('"role"')
            elif column == "data":
                columns.append('"data"')
            else:
                columns.append(column)
            
            if column in jsonb_columns:
                values.append(("'" + value + "'") if value else "null")
            elif column in ("updated","created"):
                if value:
                    value = datetime.fromtimestamp(value/1000000)
                    value = value - ist_offset
                    values.append("'" + str(value) + "'")
                else:
                    value.append("null")
            elif column in int_columns:
                values.append(value if value is not None else "null")
            elif column in boolean_columns:
                values.append(value)
            else:
                # values.append(value)
                values.append(("'" + value + "'") if value else "null")
        
        # on_conflict_action_str ->
        on_conflict_action_str = ""
        for column in columns:
            on_conflict_action_str += f"{column} = EXCLUDED.{column},"
        on_conflict_action_str = on_conflict_action_str.rstrip(",")

        # values ->
        values = [str(x) for x in values]

        query = BASE_QUERY.format(TABLE_NAME,",".join(columns),",".join(values),on_conflict_action_str)
        # print(f"perform_upsert.query => {query}")
        await db.execute_raw_insert_query(query)
    except Exception as e:
        print(f"exception.perform_upsert => {e}")
        # print(f"exception.perform_upsert.query => {query}")
        # print(f"exception.perform_upsert.failed_for_payload => {msg}")
        traceback.print_exc()
        await producer.send(topic = DLQ_PRODUCER_TOPIC,value =msg)

async def main():
    print("main")
    global consumer
    consumer = AIOKafkaConsumer(
        TOPIC,
        group_id=GROUP_ID,
        client_id=CLIENT_ID,
        auto_offset_reset="latest",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        enable_auto_commit=True,
        heartbeat_interval_ms=6000,
        session_timeout_ms=18000,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    )

    db = await init_db()
    print("db initialised")

    await consumer.start()
    print("consumer started")

    producer = AIOKafkaProducer(
        client_id = PRODUCER_CLIENT_ID,
        bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    )
    await producer.start()
    print("producer started")

    while True:
        try:
            msg = await consumer.getone()
            print(f"main.msg : {msg}")
            msg = msg.value
            msg = msg['payload']
            await perform_upsert(db,msg,producer)
        except Exception as e:
            print(f"main.exception: {str(e)}")

    await consumer.stop()


if __name__ == "__main__":
    asyncio.run(main())

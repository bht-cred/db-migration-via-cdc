from os import getenv

from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv())

write_db_name = getenv("write_db_name")
write_db_user = getenv("write_db_user")
write_db_password = getenv("write_db_password")
write_db_port = getenv("write_db_port")
write_db_url = getenv("write_db_url")
TOPIC = getenv("TOPIC")
GROUP_ID = getenv("GROUP_ID")
CLIENT_ID = getenv("CLIENT_ID")
KAFKA_BOOTSTRAP_SERVERS = getenv("KAFKA_BOOTSTRAP_SERVERS").split(",")
TABLE_NAME = getenv("TABLE_NAME","case_links")

PRODUCER_CLIENT_ID = getenv("PRODUCER_CLIENT_ID")
DLQ_PRODUCER_TOPIC = getenv("DLQ_PRODUCER_TOPIC","dlq-case_links_notice_to_recovery")

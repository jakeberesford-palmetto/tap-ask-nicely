import requests
import singer
from singer import Transformer, metadata

## Env file for testing - Change to Config for Prod
## Keep an Env for a single key in the meltano
import os
import json
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()


class SlackMessenger:
    slack_base = "https://hooks.slack.com/services"
    headers = {"Content-Type": "application/json"}

    def send_message(
        run_id: int,
        start_time: str,
        run_time: int,
        record_count: int,
        comments: str = "None",
        status: str = ":large_green_circle:",
    ):
        if comments != "None":
            status = ":red_circle:"

        message_header = f"Tap AskNicely Update:"
        message_details = f"""Tap AskNicely Update:
        - Run Id: {run_id},
        - Status: {status},
        - Start Time: {start_time},
        - Run Time: {run_time}
        - Records Synced: {record_count},
        - Comments: {comments}"""

        json_message = {
            "text": message_details,
        }

        return requests.post(
            SlackMessenger.build_url(), headers=SlackMessenger.headers, data=json.dumps(json_message)
        )

    def build_url() -> str:
        return f'{SlackMessenger.slack_base}/{os.getenv("SLACK_WEBHOOK_ADDRESS")}'


class AuditLogs:
    def audit_schema() -> dict:
        schema_base = {
            "type": ["null", "object"],
            "additionalProperties": False,
            "properties": {
                "run_id": {"type": ["null", "integer"]},
                "stream_name": {"type": ["null", "string"]},
                "batch_start": {"type": ["null", "string"], "format": "date-time"},
                "batch_end": {"type": ["null", "string"], "format": "date-time"},
                "records_synced": {"type": ["null", "integer"]},
                "run_time": {"type": ["null", "number"]},
                "comments": {"type": ["null", "string"]},
            },
        }
        return schema_base

    def schema_metadata():
        metadata = [
            {
                "breadcrumb": [],
                "metadata": {
                    "table-key-properties": [],
                    "forced-replication-method": "FULL_TABLE",
                    "inclusion": "available",
                },
            },
            {
                "breadcrumb": ["properties", "run_id"],
                "metadata": {"inclusion": "available"},
            },
            {
                "breadcrumb": ["properties", "stream_stream"],
                "metadata": {"inclusion": "available"},
            },
            {
                "breadcrumb": ["properties", "batch_start"],
                "metadata": {"inclusion": "available"},
            },
            {
                "breadcrumb": ["properties", "batch_end"],
                "metadata": {"inclusion": "available"},
            },
            {
                "breadcrumb": ["properties", "records_synced"],
                "metadata": {"inclusion": "available"},
            },
            {
                "breadcrumb": ["properties", "run_time"],
                "metadata": {"inclusion": "available"},
            },
            {
                "breadcrumb": ["properties", "comments"],
                "metadata": {"inclusion": "available"},
            },
        ]

        return metadata

    def audit_record(
        run_id: int,
        stream_name: str,
        batch_start: str,
        run_time: int,
        batch_end: str = datetime.now(),
        records_synced: int = 0,
        comments: str = "",
    ) -> dict:

        audit = {
            "run_id": run_id,
            "stream_name": stream_name,
            "batch_start": batch_start,
            "batch_end": batch_end,
            "records_synced": records_synced,
            "run_time": run_time,
            "comments": comments,
        }
        return audit

    def write_audit_log(
        run_id: int,
        stream_name: str,
        batch_start: str,
        batch_end: str = datetime.now(),
        records_synced: int = 0,
        run_time=int,
        comments: str = "",
    ):
        singer.write_schema(
            "audit_log",
            AuditLogs.audit_schema(),
            [],
            "",
        )
        audit_log = Transformer().transform(
            AuditLogs.audit_record(
                run_id=run_id,
                stream_name=stream_name,
                batch_start=batch_start,
                batch_end=batch_end,
                records_synced=records_synced,
                run_time=run_time,
                comments=comments,
            ),
            AuditLogs.audit_schema(),
            metadata.to_map(AuditLogs.schema_metadata()),
        )
        singer.write_record(
            "audit_log",
            audit_log,
        )

import requests
import singer
import logging as LOGGER
from singer import Transformer, metadata
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Content, Email, Mail
import smtplib, ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

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
        comments: str = "",
        status: str = ":large_green_circle:",
    ):
        if comments != "":
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


class SendgridMessenger:
    message = Mail(
        from_email='from_email@example.com',
        to_emails='to@example.com',
        subject='Sending with Twilio SendGrid is Fun',
        html_content='<strong>and easy to do anywhere, even with Python</strong>'
    )

    def send_message(self, message):
        try:
            sg = SendGridAPIClient(os.getenv('SENDGRID_API_KEY'))
            response = sg.send(message)

            LOGGER.info(response.status_code)
            LOGGER.info(response.body)
            LOGGER.info(response.headers)
        except Exception as error:
            LOGGER.warning(error)


class GmailMessenger:
    def __init__(self) -> None:
        self.sender_email = "info@mashey.com"
        self.receiver_email = "jordan@mashey.com"
        self.password = os.getenv('GMAIL_PW')
        self.message = MIMEMultipart("alternative")
        # Add conditional logic for a :red_circle: and :yellow_circle: emoji
        self.message["Subject"] = "Mashey | Data Sync | :large_green_circle:"
        self.message["From"] = self.sender_email
        self.message["To"] = self.receiver_email

    def create_message(self, sync_data: dict):
        # Create the plain-text and HTML version of your message
        text = f"""\
        Hi,
        It's the Mashey team with a data pipeline update.

        run_id
        stream_name
        batch_start
        batch_end
        records_synced
        run_time
        comments
        """

        html = f"""\
        <html>
        <body>
            <p>Hi,<br>
            It's the Mashey team with a data pipeline update.<br>
            <ul>
                <li>run_id</li>
                <li>stream_name</li>
                <li>batch_start</li>
                <li>batch_end</li>
                <li>records_synced</li>
                <li>run_time</li>
                <li>comments</li>
            </ul>
            <a href="http://www.mashey.com">Mashey</a><br>
            </p>
        </body>
        </html>
        """

        # Turn these into plain/html MIMEText objects
        part1 = MIMEText(text, "plain")
        part2 = MIMEText(html, "html")

        return part1, part2


    def send_message(self, sync_overview):
        part1, part2 = self.create_message(sync_overview)
        # Add HTML/plain-text parts to MIMEMultipart message
        # The email client will try to render the last part first
        self.message.attach(part1)
        self.message.attach(part2)

        # Create secure connection with server and send email
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
            server.login(self.sender_email, self.password)
            server.sendmail(
                self.sender_email, self.receiver_email, self.message.as_string()
            )

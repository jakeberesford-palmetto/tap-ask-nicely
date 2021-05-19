import requests

## Env file for testing - Change to Config for Prod
## Keep an Env for a single key in the meltano
import os
import json
from dotenv import load_dotenv

load_dotenv()


class SlackMessenger:
    slack_base = "https://hooks.slack.com/services"
    headers = {"Content-Type": "application/json"}

    def send_message(
        self,
        run_id: int,
        start_time: str,
        run_time: int,
        record_count: int,
        status: str,
        comment: str = ":large_green_circle:",
    ):
        message_header = f"Tap AskNicely Status"
        message_details = f"""
        - Run Id: {run_id},
        - Status: {status},
        - Start Time: {start_time},
        - Run Time: {run_time}
        - Records Sync'd: {record_count},
        - Comment: {comment}"""

        json_message = {
            "text": message_header,
            "blocks": [
                {"type": "section", "text": {"type": "mrkdwn", "text": message_details}}
            ],
        }

        return requests.post(self.build_url(), headers=self.headers, data=json.dumps(json_message))

    def build_url(self) -> str:
        return f'{self.slack_base}/{os.getenv("SLACK_WEBHOOK_ADDRESS")}'

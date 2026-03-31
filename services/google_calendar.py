import os
import logging
from datetime import datetime

logger = logging.getLogger(__name__)
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

SCOPES = ["https://www.googleapis.com/auth/calendar"]

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CREDENTIALS_FILE = os.path.join(BASE_DIR, "credentials.json")
TOKEN_FILE = os.path.join(BASE_DIR, "token.json")


def get_calendar_service():
    creds = None

    if os.path.exists(TOKEN_FILE):
        try:
            creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
        except Exception as e:
            logger.exception("Failed to load token.json", exc_info=e)
            creds = None

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except Exception as e:
                logger.exception("Failed to refresh token", exc_info=e)
                creds = None

        if not creds or not creds.valid:
            flow = InstalledAppFlow.from_client_secrets_file(
                CREDENTIALS_FILE,
                SCOPES
            )
            creds = flow.run_local_server(port=0)

        with open(TOKEN_FILE, "w") as token:
            token.write(creds.to_json())

    service = build("calendar", "v3", credentials=creds)
    return service


def create_event(summary: str, description: str, start_dt: datetime, end_dt: datetime) -> str:
    logger.info("calendar_event_create_started")
    try:
        service = get_calendar_service()

        event_body = {
            "summary": summary,
            "description": description,
            "start": {
                "dateTime": start_dt.isoformat(),
                "timeZone": "Europe/Kyiv",
            },
            "end": {
                "dateTime": end_dt.isoformat(),
                "timeZone": "Europe/Kyiv",
            },
        }

        event = service.events().insert(
            calendarId="primary",
            body=event_body
        ).execute()

        logger.info("calendar_event_created", extra={"calendar_event_id": event["id"]})
        return event["id"]
    except Exception as e:
        logger.exception("calendar_event_create_failed")
        raise e

def delete_event(event_id: str):
    logger.info("calendar_event_delete_started", extra={"calendar_event_id": event_id})
    try:
        service = get_calendar_service()
        service.events().delete(
            calendarId="primary",
            eventId=event_id
        ).execute()
        logger.info("calendar_event_deleted", extra={"calendar_event_id": event_id})
    except Exception as e:
        logger.exception("calendar_event_delete_failed", extra={"calendar_event_id": event_id})
        raise e
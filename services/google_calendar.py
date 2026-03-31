import os
import logging
from datetime import datetime
import asyncio
from googleapiclient.errors import HttpError

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

    return event["id"]

def delete_event(event_id: str):
    service = get_calendar_service()
    service.events().delete(
        calendarId="primary",
        eventId=event_id
    ).execute()


async def safe_create_calendar_event(summary: str, description: str, start_dt: datetime, end_dt: datetime) -> str | None:
    delays = [0, 1, 2] # 3 attempts
    logger.info("calendar_event_create_started")
    last_exception = None
    for attempt, delay in enumerate(delays, start=1):
        if attempt > 1:
            logger.warning("calendar_event_create_retry", extra={"attempt": attempt, "error": str(last_exception)})
            await asyncio.sleep(delay)
        try:
            event_id = await asyncio.to_thread(create_event, summary, description, start_dt, end_dt)
            logger.info("calendar_event_created", extra={"calendar_event_id": event_id})
            return event_id
        except HttpError as e:
            last_exception = e
            if e.resp.status in [400, 401, 403, 404]: 
                logger.exception("calendar_event_create_failed", exc_info=e)
                return None
        except Exception as e:
            last_exception = e
            
    if last_exception:
        logger.exception("calendar_event_create_failed", exc_info=last_exception)
    return None

async def safe_delete_calendar_event(event_id: str) -> bool:
    delays = [0, 1, 2]
    logger.info("calendar_event_delete_started", extra={"calendar_event_id": event_id})
    last_exception = None
    for attempt, delay in enumerate(delays, start=1):
        if attempt > 1:
            logger.warning("calendar_event_delete_retry", extra={"attempt": attempt, "error": str(last_exception), "calendar_event_id": event_id})
            await asyncio.sleep(delay)
        try:
            await asyncio.to_thread(delete_event, event_id)
            logger.info("calendar_event_deleted", extra={"calendar_event_id": event_id})
            return True
        except HttpError as e:
            last_exception = e
            if e.resp.status in [400, 401, 403, 404]:
                logger.exception("calendar_event_delete_failed", extra={"calendar_event_id": event_id}, exc_info=e)
                return False
        except Exception as e:
            last_exception = e
            
    if last_exception:
        logger.exception("calendar_event_delete_failed", extra={"calendar_event_id": event_id}, exc_info=last_exception)
    return False
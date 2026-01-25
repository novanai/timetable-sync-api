import datetime
import enum
import uuid

import aiohttp
import icalendar
import msgspec
from rapidfuzz import fuzz, process
from rapidfuzz import utils as fuzz_utils

from timetable import __version__

SITE = "dcuclubsandsocs.ie"


class GroupType(enum.Enum):
    """The group type."""

    CLUB = "club"
    """A club."""
    SOCIETY = "society"
    """A society."""


class Event(msgspec.Struct):
    """An event."""

    name: str
    """The event name."""
    image: str | None
    """The event poster."""
    start: datetime.datetime
    """The event's start time."""
    end: datetime.datetime
    """The event's end time."""
    day: str
    """The day the event is on (`monday`, `tuesday`, etc.)."""
    cost: float
    """The event cost."""
    capacity: int | None
    """The event maximum capacity."""
    type: str
    """The event type. Usually `IN-PERSON` or `VIRTUAL`."""
    location: str | None
    """The event location."""
    description: str
    """The event description."""


class Activity(msgspec.Struct):
    """A weekly activity."""

    name: str
    """The activity name."""
    image: str | None
    """The activity poster."""
    day: str
    """The day the activity is on (`monday`, `tuesday`, etc.)."""
    start: datetime.datetime
    """The activity start time."""
    end: datetime.datetime
    """The activity end time."""
    capacity: int | None
    """The activity maximum capacity."""
    type: str
    """The activity type. Usually `IN-PERSON` or `VIRTUAL`."""
    location: str | None
    """The activity location."""
    description: str
    """The activity description."""


class Fixture(msgspec.Struct):
    """A fixture."""

    name: str
    """The fixture name."""
    image: str | None
    """The fixture poster."""
    start: datetime.datetime
    """The fixture's start time."""
    competition: str
    """The fixture competition."""
    type: str
    """The fixture type. Usually `HOME` or `AWAY`."""
    location: str | None
    """The fixture location."""
    description: str
    """The fixture description."""


class ClubSoc(msgspec.Struct):
    """A club or society."""

    id: str
    """The ID used in the club or society's page URL."""
    name: str
    """The club or society name."""


class API:
    def __init__(self, cns_address: str) -> None:
        self.cns_address = cns_address
        self._session: aiohttp.ClientSession | None = None

    @property
    def session(self) -> aiohttp.ClientSession:
        """The `aiohttp.ClientSession` to use for API requests."""
        if not self._session:
            self._session = aiohttp.ClientSession()
        return self._session

    async def get_data(self, url: str) -> bytes:
        async with self.session.request("GET", f"{self.cns_address}/{url}") as r:
            r.raise_for_status()
            return await r.read()

    async def fetch_group_events_activities_fixtures(
        self,
        group_type: GroupType,
        identity: str,
    ) -> list[Activity | Event | Fixture]:
        events = msgspec.json.decode(
            await self.get_data(f"{SITE}/{group_type.value}/{identity}/events"),
            type=list[Event],
        )
        activities = msgspec.json.decode(
            await self.get_data(f"{SITE}/{group_type.value}/{identity}/activities"),
            type=list[Activity],
        )
        fixtures = msgspec.json.decode(
            await self.get_data(f"{SITE}/{group_type.value}/{identity}/fixtures"),
            type=list[Fixture],
        )

        return [*events, *activities, *fixtures]

    async def fetch_group_items(self, group_type: GroupType) -> list[ClubSoc]:
        return msgspec.json.decode(
            await self.get_data(f"{SITE}/{group_type.value}"),
            type=list[ClubSoc],
        )

    async def fetch_item(self, group_type: GroupType, identity: str) -> ClubSoc:
        return msgspec.json.decode(
            await self.get_data(f"{SITE}/{group_type.value}/{identity}"), type=ClubSoc
        )


def filter_group_items(items: list[ClubSoc], query: str) -> list[ClubSoc]:
    names = [item.name for item in items]

    matches = process.extract(
        query,
        names,
        scorer=fuzz.partial_ratio,
        processor=fuzz_utils.default_process,
        score_cutoff=80,
    )

    return [items[idx] for _, _, idx in matches]


def generate_ical_file(events: dict[str, list[Event | Activity | Fixture]]) -> bytes:
    calendar = icalendar.Calendar()
    calendar.add("METHOD", "PUBLISH")
    calendar.add(
        "PRODID", f"-//timetable.redbrick.dcu.ie//TimetableSync {__version__}//EN"
    )
    calendar.add("VERSION", "2.0")
    calendar.add("DTSTAMP", datetime.datetime.now(datetime.timezone.utc))

    for group_name, group_events in events.items():
        for item in group_events:
            event = icalendar.Event()
            event.add("UID", uuid.uuid4())
            event.add("LAST-MODIFIED", item.start)
            event.add("DTSTART", item.start)
            if isinstance(item, (Event, Activity)):
                event.add("DTEND", item.end)
            event.add("DTSTAMP", item.start)
            event.add("SUMMARY", f"{item.name} [{group_name}]")
            if item.description.strip():
                event.add(
                    "DESCRIPTION",
                    (
                        f"Details: {item.description}\n"
                        + (
                            f"Cost: {f'€{item.cost:.2f}' if item.cost else 'FREE'}"
                            if isinstance(item, Event)
                            else ""
                        )
                    ).strip(),
                )
            if item.location is not None:
                event.add("LOCATION", item.location)
            event.add("CLASS", "PUBLIC")
            calendar.add_component(event)

    return calendar.to_ical()

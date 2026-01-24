from __future__ import annotations

import collections
import dataclasses
import datetime
import logging
import re
import typing

import icalendar

from timetable import __version__, models

if typing.TYPE_CHECKING:
    from timetable import api as api_


logger = logging.getLogger(__name__)


SMALL_WORDS = re.compile(
    r"\b(a|an|and|at|but|by|de|en|for|if|in|of|on|or|the|to|via|vs?\.?)\b",
    re.IGNORECASE,
)
INTERNAL_CAPS = re.compile(r"\S+[A-Z]+\S*")
SPLIT_ON_WHITESPACE = re.compile(r"\s+")
SPLIT_ON_HYPHENS = re.compile(r"-")


def parse_weeks(weeks: str) -> list[int]:
    """Parse a weeks string into a list of week numbers."""
    groups = [w.strip() for w in weeks.split(",")]

    final: list[int] = []

    for w in groups:
        w = w.split("-")  # noqa: PLW2901

        if len(w) == 1:
            final.append(int(w[0]))
        elif len(w) == 2:
            final.extend(list(range(int(w[0]), int(w[1]) + 1)))

    return final


# TODO: this might need to include summer dates for those modules?
def default_year_start_end_dates() -> tuple[datetime.datetime, datetime.datetime]:
    """Get default start and end dates for the academic year.

    * Default start date: Aug 1
    * Default end date: May 1

    Returns
    -------
    tuple[datetime.datetime, datetime.datetime]
        The start and end dates.
    """
    now = datetime.datetime.now(datetime.timezone.utc)
    start_year = now.year if now.month >= 8 else now.year - 1
    end_year = now.year + 1 if now.month >= 8 else now.year
    start = datetime.datetime(start_year, 8, 1, tzinfo=datetime.timezone.utc)
    end = datetime.datetime(end_year, 5, 1, tzinfo=datetime.timezone.utc)
    return (start, end)


# TODO: rename to something that makes sense
def calc_start_end_range(
    start: datetime.datetime | None = None, end: datetime.datetime | None = None
) -> tuple[datetime.datetime, datetime.datetime]:
    start_default, end_default = default_year_start_end_dates()
    start = (start or start_default).astimezone(datetime.UTC)
    end = (end or end_default).astimezone(datetime.UTC)

    # TODO: is this warning necessary?
    if not (start_default <= start <= end_default) or not (
        start_default <= end <= end_default
    ):
        logger.warning(
            f"{start} and {end} datetimes not within the current academic year range"
        )

    # TODO: set end to start + 1 week if end before start
    if start > end:
        raise ValueError("Start date/time cannot be later than end date/time")

    return start, end


@dataclasses.dataclass
class BasicCategoryItem:
    name: str
    identity: str


async def get_basic_category_items(
    api: api_.API,
    category_type: models.CategoryType,
    query: str | None = None,
) -> list[BasicCategoryItem]:
    result = await api.get_category(category_type, query=query)
    if not result:
        result = await api.fetch_category(category_type, query=query, cache=True)

    return [BasicCategoryItem(name=c.name, identity=c.identity) for c in result.items]


async def resolve_to_category_items(
    original_codes: dict[models.CategoryType, list[str]],
    api: api_.API,
) -> dict[models.CategoryType, list[models.CategoryItem]]:
    codes: dict[models.CategoryType, list[models.CategoryItem]] = (
        collections.defaultdict(list)
    )

    for group, cat_codes in original_codes.items():
        for code in cat_codes:
            # code is a category item identity and timetable must be fetched
            item = await api.get_category_item(code)
            if item:
                codes[group].append(item)
                continue

            # code is not a category item, search cached category items for it
            category = await api.get_category(group, query=code, count=1)
            if not category or not category.items:
                # could not find category item in cache, fetch it
                category = await api.fetch_category(group, query=code)
                if not category.items:
                    raise models.InvalidCodeError(code)

            item = category.items[0]
            codes[group].append(item)

    return codes


async def gather_events(
    group_identities: dict[models.CategoryType, list[str]],
    start_date: datetime.datetime | None,
    end_date: datetime.datetime | None,
    api: api_.API,
) -> list[models.Event]:
    events: list[models.Event] = []

    for group, identities in group_identities.items():
        for identity in identities:
            # timetable is cached
            timetable = await api.get_category_item_timetable(
                identity, start=start_date, end=end_date
            )
            if timetable:
                events.extend(timetable.events)
                continue

            # TODO: make a group_identities dict and fetch in one request
            # timetable needs to be fetched
            timetables = await api.fetch_category_items_timetables(
                group,
                [identity],
                start=start_date,
                end=end_date,
            )
            events.extend(timetables[0].events)

    return events


# Converted to python and modified from
# https://github.com/HubSpot/humanize/blob/master/src/humanize.js#L439-L475
def title_case(text: str) -> str:
    def do_title_case(
        _text: str, hyphenated: bool = False, first_or_last: bool = True
    ) -> str:
        title_cased_array: list[str] = []
        string_array = re.split(
            SPLIT_ON_HYPHENS if hyphenated else SPLIT_ON_WHITESPACE, _text
        )

        for index, word in enumerate(string_array):
            if "-" in word:
                title_cased_array.append(
                    do_title_case(
                        word, True, index == 0 or index == len(string_array) - 1
                    )
                )
                continue

            if first_or_last and (index == 0 or index == len(string_array) - 1):
                title_cased_array.append(
                    word.capitalize() if not INTERNAL_CAPS.search(word) else word
                )
                continue

            if INTERNAL_CAPS.search(word):
                title_cased_array.append(word)
            elif SMALL_WORDS.search(word):
                title_cased_array.append(word.lower())
            else:
                title_cased_array.append(word.capitalize())

        return (
            "-".join(title_cased_array) if hyphenated else " ".join(title_cased_array)
        )

    return do_title_case(text)


# TODO: rename to 'create'
def generate_ical_file(events: list[models.Event]) -> bytes:
    calendar = icalendar.Calendar()
    calendar.add("METHOD", "PUBLISH")
    calendar.add(
        "PRODID", f"-//timetable.redbrick.dcu.ie//TimetableSync {__version__}//EN"
    )
    calendar.add("VERSION", "2.0")
    calendar.add("DTSTAMP", datetime.datetime.now(datetime.timezone.utc))

    for item in events:
        event = icalendar.Event()
        event.add("UID", item.identity)
        event.add("LAST-MODIFIED", item.last_modified)
        event.add("DTSTART", item.start)
        event.add("DTEND", item.end)
        event.add("DTSTAMP", item.last_modified)
        event.add("SUMMARY", item.extras.summary_long)
        event.add(
            "DESCRIPTION",
            f"Details: {item.description}\nStaff: {item.staff_member}",
        )
        event.add("LOCATION", item.extras.location_long)
        event.add("CLASS", "PUBLIC")
        calendar.add_component(event)

    return calendar.to_ical()

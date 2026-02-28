from __future__ import annotations

import collections
import datetime
import logging
import re
import typing
from uuid import UUID

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
        raise ValueError("Start datetime cannot be later than end datetime")

    return start, end


async def resolve_to_category_items(
    original_codes: dict[models.CategoryType, list[str]],
    api: api_.API,
) -> dict[models.CategoryType, list[models.BasicCategoryItem]]:
    async def resolve_from_uuid(
        category_type: models.CategoryType, item_id: UUID
    ) -> models.BasicCategoryItem:
        return await api.get_category_item(item_id) or await api.fetch_category_item(
            category_type, item_id
        )

    async def resolve_from_code(
        category_type: models.CategoryType, code: str
    ) -> models.BasicCategoryItem | None:
        # try from cache
        category = await api.get_category(
            category_type, query=code, limit=1, items_type=models.BasicCategoryItem
        )
        if category and category.items:
            return category.items[0]

        # fallback to fetch
        category = await api.fetch_category(
            category_type, query=code, items_type=models.BasicCategoryItem
        )
        if category.items:
            return category.items[0]

        return None

    codes: dict[models.CategoryType, list[models.BasicCategoryItem]] = (
        collections.defaultdict(list)
    )

    for group, cat_codes in original_codes.items():
        for code in cat_codes:
            try:
                item_id = UUID(code)
                item = await resolve_from_uuid(group, item_id)
            except ValueError:
                item = await resolve_from_code(group, code)
                if not item:
                    raise models.InvalidCodeError(code)

            codes[group].append(item)

    return codes


async def gather_events(
    group_identities: dict[models.CategoryType, list[UUID]],
    start_date: datetime.datetime | None,
    end_date: datetime.datetime | None,
    api: api_.API,
) -> list[models.Event]:
    timetables_to_fetch: dict[models.CategoryType, list[UUID]] = (
        collections.defaultdict(list)
    )
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

            timetables_to_fetch[group].append(identity)

    for group, identities in timetables_to_fetch.items():
        timetables = await api.fetch_category_items_timetables(
            group,
            identities,
            start=start_date,
            end=end_date,
        )
        for timetable in timetables:
            events.extend(timetable.events)

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


def to_ics_file(events: list[models.Event]) -> bytes:
    def format_datetime(dt: datetime.datetime) -> str:
        """Format datetime for ics format. This assumes the datetime is in UTC."""
        return dt.strftime("%Y%m%dT%H%M%SZ")

    def format_text(text: str) -> str:
        """Format text for ics format."""
        return (
            text.replace(r"\N", "\n")
            .replace("\\", "\\\\")
            .replace(";", r"\;")
            .replace(",", r"\,")
            .replace("\r\n", r"\n")
            .replace("\n", r"\n")
        )

    parts: list[str] = [
        "BEGIN:VCALENDAR\n",
        "VERSION:2.0\n",
        "METHOD:PUBLISH\n",
        f"PRODID:-//timetable.redbrick.dcu.ie//TimetableSync {__version__}//EN\n",
        f"DTSTAMP:{format_datetime(datetime.datetime.now(datetime.timezone.utc))}\n",
    ]

    for item in events:
        parts.append(
            "BEGIN:VEVENT\n"
            f"UID:{item.identity}\n"
            f"DTSTAMP:{format_datetime(item.last_modified)}\n"
            f"LAST-MODIFIED:{format_datetime(item.last_modified)}\n"
            f"DTSTART:{format_datetime(item.start)}\n"
            f"DTEND:{format_datetime(item.end)}\n"
            f"SUMMARY:{format_text(item.extras.summary_long)}\n"
            f"DESCRIPTION:{format_text(f'Details: {item.description or "[unknown]"}\nStaff: {item.staff_member or "[unknown]"}')}\n"
            f"LOCATION:{format_text(item.extras.location_long)}\n"
            "CLASS:PUBLIC\n"
            "END:VEVENT\n"
        )

    parts.append("END:VCALENDAR\n")

    return "".join(parts).encode("utf-8")

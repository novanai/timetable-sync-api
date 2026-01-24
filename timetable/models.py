from __future__ import annotations

import collections
import datetime
import enum
import logging
import re
import typing

import msgspec

from timetable import types, utils

logger = logging.getLogger(__name__)

T = typing.TypeVar("T", bound="PayloadModel")

LOCATION_REGEX = re.compile(
    r"^((?P<campus>[A-Z]{3})\.)?(?P<building>VB|[A-Z][AC-FH-Z]?)(?P<floor>[BG1-9])(?P<room>[0-9\-A-Za-z ()]+)$"
)

EVENT_NAME_REGEX = re.compile(
    r"(?P<modules_semester>[A-Z]{2,3}\d{4}(?:\/(?:[A-Z]{2,3}\d{4}|\d{2,4}))*(?:\[(?:1|1,2|2|2,3|3|3,1|TM|AY)\][A-Z]{2,3}\d{4})*\[(?:1|1,2|2|2,3|3|3,1|TM|AY)\])(?:(?P<delivery_type>OC|0C|AY|AS|ASY|SY|HY)\/)?(?:(?P<activity_type>[PLTWSE])\d{1,2})[^\/\n]*(?:\/(?P<group_number>\d{2}))?"
)

MODULES_SEMESTER_VERSION_1 = re.compile(
    r"^(?P<modules>(?:[A-Z]{2,3}\d{4}\/?)+)\[(?P<semester>1|1,2|2|2,3|3|3,1|TM|AY)\]$"
)
"""
matches:
- HIS1080/HIS1076[2]
- HIS1080/HIS1076[2]
"""

# NOTE: this will do a partial match on version 1, so you MUST match
# with version 1 first and only use version 2 if there were no matches
MODULES_SEMESTER_VERSION_2 = re.compile(
    r"(?P<module>[A-Z]{2,3}\d{4})\[(?P<semester>1|1,2|2|2,3|3|3,1|TM|AY)\]"
)
"""
matches:
- HIS1013[2]HIS1014[2]
- HIS1013[2]HIS1014[2]
"""

MODULES_SEMESTER_VERSION_3 = re.compile(
    r"^(?P<name>[A-Z]{3})(?P<codes>\d{4}\/(?:\d{4}|\d{2}))\[(?P<semester>1|1,2|2|2,3|3|3,1|TM|AY)\]$"
)
"""
matches:
- TRA1017/1018[2]OC/L1/01
- SPA1035/30[1]OC/L1/01
"""

SEMESTER_CODE = re.compile(r"\[[1|1,2|2|2,3|3|3,1|TM|AY]\]")

EVENT_NAME_SUBSTITUTIONS: dict[str, str] = {
    " ": "",
    "//": "/",
    "(": "[",
    ")": "]",
    "{": "[",
    "}": "]",
    "]/": "]",
    "][": "]",
    "[[": "[",
    "]]": "]",
    "/]": "/",
    "[/": "/",
}

FLOOR_ORDER: typing.Final[str] = "BG123456789"

CAMPUSES = {"AHC": "All Hallows", "GLA": "Glasnevin", "SPC": "St Patrick's"}

BUILDINGS = {
    "GLA": {
        "A": "Albert College",
        "B": "Invent Building",
        "C": "Henry Grattan Building",
        "CA": "Henry Grattan Extension",
        "D": "BEA Orpen Building",
        "E": "Estates Office",
        "F": "Multi-Storey Car Park",
        "FT": "The Polaris Building",
        "G": "NICB Building",
        "GA": "NRF Building",
        "H": "Nursing Building",
        "J": "Hamilton Building",
        "KA": "U Building / Student Centre",
        "L": "McNulty Building",
        "M": "Interfaith Centre",
        "N": "Marconi Building",
        "P": "Pavilion",
        "PR": "Restaurant",
        "Q": "Business School",
        "QA": "MacCormac Reception",
        "R": "Creche",
        "S": "Stokes Building",
        "SA": "Stokes Annex",
        "T": "Terence Larkin Theatre",
        "U": "Accommodation & Sports Club",
        "V1": "Larkfield Residences",
        "V2": "Hampstead Residences",
        "VA": "Postgraduate Residences A",
        "VB": "Postgraduate Residences B",
        "W": "College Park Residences",
        "X": "Lonsdale Building",
        "Y": "O'Reilly Library",
        "Z": "The Helix",
    },
    "SPC": {
        "A": "Block A",
        "B": "Block B",
        "C": "Block C",
        "D": "Block D",
        "E": "Block E",
        "F": "Block F",
        "G": "Block G",
        "S": "Block S / Sports Hall",
    },
    "AHC": {
        "C": "Chapel",
        "OD": "O'Donnell House",
        "P": "Purcell House",
        "S": "Senior House",
    },
}


class CategoryType(enum.Enum):
    """A category type."""

    MODULES = "525fe79b-73c3-4b5c-8186-83c652b3adcc"
    """Modules."""
    LOCATIONS = "1e042cb1-547d-41d4-ae93-a1f2c3d34538"
    """Locations."""
    PROGRAMMES_OF_STUDY = "241e4d36-60e0-49f8-b27e-99416745d98d"
    """Programmes of Study (Courses)."""


class DisplayEnum(enum.Enum):
    """Enum with method for displaying the value in a proper format."""

    @property
    def display(self) -> str:
        """Proper format for enum value."""
        return self.name.replace("_", " ").title()


class Semester(DisplayEnum):
    """The semester."""

    SEMESTER_1 = "1"
    """Semester 1."""
    SEMESTER_2 = "2"
    """Semester 2."""
    SEMESTER_3 = "3"
    """Semester 3."""
    SEMESTER_1_AND_2 = "1,2"
    """Semesters 1 and 2."""
    SEMESTER_1_AND_3 = "1,3"
    """Semesters 1 and 3."""
    SEMESTER_2_AND_3 = "2,3"
    """Semesters 2 and 3."""
    YEAR_LONG = "AY"
    """Year Long (Semesters 1, 2 and 3)."""
    TWELVE_MONTH = "TM"
    """Twelve Month."""


class DeliveryType(DisplayEnum):
    """Delivery type of an event."""

    ON_CAMPUS = "OC"
    """On campus."""
    ASYNCHRONOUS = "AY"
    """Asynchronous (recorded)."""
    SYNCHRONOUS = "SY"
    """Synchronous (online, live)."""
    HYBRID = "HY"
    """Hybrid (both on-campus and online)."""

    @property
    def display(self) -> str:
        return DELIVERY_TYPES[self]


DELIVERY_TYPES: dict[DeliveryType, str] = {
    DeliveryType.ON_CAMPUS: "On Campus",
    DeliveryType.ASYNCHRONOUS: "Asynchronous (Recorded)",
    DeliveryType.SYNCHRONOUS: "Synchronous (Online, live)",
    DeliveryType.HYBRID: "Hybrid",
}


class ActivityType(DisplayEnum):
    """Activity type of an event."""

    PRACTICAL = "P"
    """Practical."""
    LECTURE = "L"
    """Lecture."""
    TUTORIAL = "T"
    """Tutorial."""
    WORKSHOP = "W"
    """Workshop."""
    SEMINAR = "S"
    """Seminar."""
    EXAMINATION = "E"
    """Examination."""


class PayloadModel(typing.Protocol):
    """Base payload protocol."""

    @classmethod
    def from_payload(cls, payload: dict[str, typing.Any]) -> typing.Self: ...


class FromPayloadsMixin:
    @classmethod
    def from_payloads(
        cls: type[T], payloads: typing.Sequence[dict[str, typing.Any]]
    ) -> list[T]:
        return [cls.from_payload(p) for p in payloads]


class Category(msgspec.Struct):
    """Information about a category."""

    items: list[CategoryItem]
    """The category items."""
    count: int
    """The number of items in this category."""

    @classmethod
    def from_payload(cls, payload: dict[str, typing.Any]) -> typing.Self:
        return cls(
            items=CategoryItem.from_payloads(payload["Results"]),
            count=payload["Count"],
        )


class CategoryItem(FromPayloadsMixin, msgspec.Struct):
    """An item belonging to a category. This could be a course, module or location."""

    description: str | None
    """- For courses, this is the full title of the course.
    - For modules, this is either the full title of the module or `None`.
    - For locations, this is a brief description of the location.
    In the cases of description being `None`, `CategoryItem.name` should be used.
    ### Examples
    - Courses: `"BSc in Computer Science"`
    - Modules: `"Computer Programming I"` / `None`
    - Locations: `"Tiered Lecture Theatre"`
    """
    category_type: CategoryType
    """The type of category this item belongs to."""
    parent_categories: list[str]
    """Unique identities of the faculty(s) this item belongs to."""
    identity: str
    """Unique identity of this category item."""
    name: str
    """- For courses, this is the course code.
    - For modules, this is the full module name, including the code, semester and full title.
    - For locations, this is the location's code, which can be parsed by `Location.from_str`
    ### Examples:
    - Courses: `"COMSCI1"`
    - Modules: `"CSC1003[1] Computer Programming I"`
    - Locations: `"GLA.C117 & C122"`
    """
    code: str
    """The course, module or location code(s).
    If this is for a location, it may contain multiple codes separated by a space.
    ### Examples:
    - Courses: `"COMSCI1"`
    - Modules: `"CSC1003[1]"`
    - Locations: `"GLA.C117 GLA.C122"`
    """

    @classmethod
    def from_payload(cls, payload: dict[str, typing.Any]) -> typing.Self:
        cat_type = CategoryType(payload["CategoryTypeIdentity"])
        name: str = payload["Name"]

        if cat_type is CategoryType.LOCATIONS:
            locations = Location.from_str(name)
            code = " ".join([str(loc) for loc in locations])
        else:
            code = name.split(" ")[0]

        return cls(
            description=payload["Description"].strip() or None,
            category_type=cat_type,
            parent_categories=payload["ParentCategoryIdentities"],
            identity=payload["Identity"],
            name=name,
            code=code.strip(),
        )


class CategoryItemTimetable(msgspec.Struct):
    """A category item's timetable."""

    category_type: CategoryType
    """The type of category this timetable is for."""
    identity: str
    """The identity of the category item this timetable is for."""
    name: str
    """- For courses, this is the course code.
    - For modules, this is the full module name, including the code, semester and full title.
    - For locations, this is the location code.
    ### Examples
    - Courses: `"COMSCI1"`
    - Modules: `"CSC1003[1] Computer Programming I"`
    - Locations: `"GLA.L129"`
    """
    events: list[Event]
    """Events on this timetable."""

    @classmethod
    def from_payload(cls, payload: dict[str, typing.Any]) -> typing.Self:
        return cls(
            category_type=payload["CategoryTypeIdentity"],
            identity=payload["Identity"],
            name=payload["Name"],
            events=Event.from_payloads(payload["Results"]),
        )


class Event(FromPayloadsMixin, msgspec.Struct):
    """A timetabled event."""

    identity: str
    """Unique identity of the event."""
    start: datetime.datetime
    """Start time of the event."""
    end: datetime.datetime
    """End time of the event."""
    status_identity: str
    """This appears to be an identity shared between events of the same activity type and number.
    ### Examples
    - L1 (Lecture 1) all share the same identity
    - T3 (Tutorial 3) all share the same identity (but a different identity to L1)
    """
    locations: list[Location] | None
    """A list of locations for this event, or `None` if there are no locations
    (e.g. for asynchronous events).
    """
    description: str | None
    """A description of the event. This could be anything from the activity type
    (e.g. `"Lecture"`) to a brief description of the event (e.g. `"Introduction to Computing"`)
    and should therefore not be relied on to provide consistent information.
    """
    name: str
    """The name of the event."""
    event_type: str
    """The activity type, almost always `"On Campus"`, `"Synchronous (Online, live)"`,
    `"Asynchronous (Recorded)"` or `"Booking"`.
    """
    last_modified: datetime.datetime
    """The last time this event was modified."""
    module_name: str | None
    """The full module name.
    ### Example
    CSC1003[1] Computer Programming I
    """
    staff_member: str | None
    """The event's staff member's name.
    ### Example
    Blott S
    """
    weeks: list[int] | None
    """List of week numbers this event takes place on."""
    group_name: str | None
    """The group name, if parsed from either the event name or description."""
    extras: ExtraEventData
    """Additional data for public display."""

    @classmethod
    def from_payload(cls, payload: dict[str, typing.Any]) -> typing.Self:
        description: str | None = payload["Description"].strip() or None
        name: str = payload["Name"]
        event_type: str = payload["EventType"]

        locations = (
            Location.from_str(loc) if (loc := payload["Location"]) is not None else []
        )

        module_name: str | None = None
        staff_member: str | None = None
        weeks: list[int] | None = None

        for item in payload["ExtraProperties"]:
            rank = item["Rank"]
            if rank == 1:
                module_name = item["Value"]
            elif rank == 2:
                staff_member = item["Value"]
            elif rank == 3:
                weeks = utils.parse_weeks(item["Value"])

        group_name: str | None = None
        for grp in ("group", "grp"):
            for value in (payload["Name"], payload["Description"]):
                value: str = value.lower().replace(" ", "")  # noqa: PLW2901
                if grp in value and (index := value.index(grp) + len(grp)) < len(value):
                    group_name = value[index].upper()
                    break

        extras = ExtraEventData.from_event(
            name=name,
            event_type=event_type,
            description=description,
            module_name=module_name,
            group_name=group_name,
            locations=locations,
        )

        return cls(
            identity=payload["Identity"],
            start=datetime.datetime.fromisoformat(payload["StartDateTime"]),
            end=datetime.datetime.fromisoformat(payload["EndDateTime"]),
            status_identity=payload["StatusIdentity"],
            locations=locations,
            description=description,
            name=name,
            event_type=event_type,
            last_modified=datetime.datetime.fromisoformat(payload["LastModified"]),
            module_name=module_name,
            staff_member=staff_member,
            weeks=weeks,
            group_name=group_name,
            extras=extras,
        )


class EventNameData(msgspec.Struct):
    """Data parsed from the event name into proper formats."""

    module_codes: list[str]
    """A list of module codes this event is for.
    ### Example
    `["PS114", "PS114A"]`
    """
    semester: Semester
    """The semester this event takes place in."""
    delivery_type: DeliveryType | None
    """The delivery type of this event. May be `None`."""
    activity_type: ActivityType
    """The activity type of this event."""
    group_number: int | None
    """The group this event is for. May be `None`."""

    @classmethod
    def from_str(cls, data: str) -> list[EventNameData]:
        # Some error correction
        data = data.upper()
        for original, substitution in EVENT_NAME_SUBSTITUTIONS.items():
            data = data.replace(original, substitution)

        matches: list[EventNameData] = []

        def get_modules(module_str: str) -> list[str]:
            return [module for module in module_str.split("/") if module.strip()]

        def get_module_codes(name: str, codes: list[str]) -> list[str]:
            base_code = codes[0][:2]
            modules: list[str] = []

            for code in codes:
                if len(code) == 2:
                    modules.append(f"{name}{base_code}{code}")
                else:
                    assert len(code) == 4
                    modules.append(f"{name}{code}")

            return modules

        def get_semester(semester_str: str) -> Semester:
            return Semester(semester_str)

        def get_delivery_type(deliver_str: str | None) -> DeliveryType | None:
            if not deliver_str:
                return None

            if deliver_str == "0C":
                deliver_str = "OC"
            elif deliver_str in {"AS", "ASY"}:
                deliver_str = "AY"

            return DeliveryType(deliver_str)

        def get_activity_type(activity_str: str) -> ActivityType:
            return ActivityType(activity_str)

        def get_group_number(group_str: str | None) -> int | None:
            return int(group_str) if group_str else None

        for match in EVENT_NAME_REGEX.finditer(data):
            modules_semester = match.group("modules_semester")
            delivery_type = get_delivery_type(match.group("delivery_type"))
            activity_type = get_activity_type(match.group("activity_type"))
            group_number = get_group_number(match.group("group_number"))

            modules: list[str] | None = None
            semester: Semester | None = None

            if ms_match := MODULES_SEMESTER_VERSION_1.match(modules_semester):
                modules = get_modules(ms_match.group("modules"))
                semester = get_semester(ms_match.group("semester"))

            elif not ms_match and (
                ms_match := list(MODULES_SEMESTER_VERSION_2.finditer(modules_semester))
            ):
                modules = []
                semesters: list[str] = []
                for ms in ms_match:
                    modules.append(ms.group("module"))
                    semesters.append(ms.group("semester"))

                assert (
                    len(set(semesters)) <= 1
                )  # semesters should contain a single unique value
                semester = get_semester(semesters[0])

            elif not ms_match and (
                ms_match := MODULES_SEMESTER_VERSION_3.match(modules_semester)
            ):
                modules = get_module_codes(
                    ms_match.group("name"),
                    ms_match.group("codes").split("/"),
                )
                semester = get_semester(ms_match.group("semester"))

            assert modules is not None and semester is not None

            matches.append(
                cls(
                    module_codes=modules,
                    semester=semester,
                    delivery_type=delivery_type,
                    activity_type=activity_type,
                    group_number=group_number,
                )
            )

        if not matches:
            logger.warning(f"Failed to parse: {data}")

        return matches


class ExtraEventData(msgspec.Struct):
    """Display data for events."""

    event_name_data: list[EventNameData]
    """Data parsed from the event name into proper formats.
    May be an empty list (if parsing was unsuccessful).
    """
    summary: str
    """Short summary of this event."""
    summary_long: str
    """Long summary of this event."""
    location: str
    """Location(s) of this event."""
    location_long: str
    """Long Location(s) of this event."""
    description: str
    """Description of this event."""

    # TODO: split into sub-methods
    @classmethod
    def from_event(
        cls,
        name: str,
        event_type: str,
        description: str | None,
        module_name: str | None,
        group_name: str | None,
        locations: list[Location],
    ) -> typing.Self:
        event_name_data = EventNameData.from_str(name)

        # SUMMARY

        name = re.sub(SEMESTER_CODE, "", n) if (n := module_name) else name

        if description and description.lower().strip() == "lab":
            activity = "Lab"
        elif event_name_data:
            activity = event_name_data[0].activity_type.display
        else:
            activity = None

        if activity and group_name:
            summary_long = f"({activity}, Group {group_name})"
        elif activity:
            summary_long = f"({activity})"
        elif group_name:
            summary_long = f"(Group {group_name})"
        else:
            summary_long = None

        summary_long = utils.title_case(
            (name + (f" {summary_long}" if summary_long else "")).strip()
        )
        summary_short = utils.title_case(name)
        if group_name:
            summary_short = f"{summary_short} (Group {group_name})".strip()

        # LOCATIONS

        if locations:
            # dict[(campus, building)] = [locations]  # noqa: ERA001
            locations_: dict[tuple[str, str] | None, list[Location]] = (
                collections.defaultdict(list)
            )

            for loc in locations:
                if loc.original is not None:
                    locations_[None].append(loc)
                else:
                    locations_[(loc.campus, loc.building)].append(loc)

            locations_long: list[str] = []
            locations_short: list[str] = []
            for main, locs in locations_.items():
                if main is None:
                    locs_ = [loc.original for loc in locs]
                    assert types.is_str_list(locs_)
                    loc_string = ", ".join(locs_)
                    locations_long.append(loc_string)
                    locations_short.append(loc_string)
                    continue

                campus, building = main
                building = BUILDINGS[campus].get(building, "[unknown]")
                campus = CAMPUSES[campus]
                locs = sorted(locs, key=lambda r: r.room)  # noqa: PLW2901
                locs = sorted(locs, key=lambda r: FLOOR_ORDER.index(r.floor))  # noqa: PLW2901
                locations_long.append(
                    f"{', '.join((f'{loc.building}{loc.floor}{loc.room}' for loc in locs))} ({building}, {campus})"
                )
                locations_short.append(
                    f"{', '.join((f'{loc.building}{loc.floor}{loc.room}' for loc in locs))}"
                )

            location_long = ", ".join(locations_long)
            location_short = ", ".join(locations_short)
        else:
            location_long = event_type
            location_short = event_type

        # DESCRIPTION

        event_type = (
            data[0].delivery_type.display
            if (data := event_name_data) and data[0].delivery_type is not None
            else event_type
        )
        if event_type.lower().strip() == "booking":
            description = f"{description}, {event_type}" if description else event_type
        else:
            description = f"{activity}, {event_type}" if activity else event_type

        return cls(
            event_name_data=event_name_data,
            summary=summary_short,
            summary_long=summary_long,
            location=location_short,
            location_long=location_long,
            description=description,
        )


class Location(msgspec.Struct):
    """A location."""

    campus: str
    """The campus code.
    ### Allowed Values
    `"GLA"`, `"SPC"`, `"AHC"`
    """
    building: str
    """The building code.
    ### Examples
    `"L"`, `"SA"`
    """
    floor: str
    """The floor code.
    ### Allowed Values
    `"B"` - Basement, `"G"` - Ground Floor, `number > 0` - Floor Number
    """
    room: str
    """The room code. Not guaranteed to be just a number."""
    original: str | None = None
    """The original location code. If `None`, the location was parsed correctly."""

    @classmethod
    def from_str(cls, location: str) -> list[Location]:
        locations: list[str] = []

        for loc in location.split(","):
            loc = loc.strip()  # noqa: PLW2901
            if "&" in loc:
                campus, rooms = loc.split(".")
                rooms = [r.strip() for r in rooms.split("&")]
                locations.extend(f"{campus}.{room}" for room in rooms)
            else:
                locations.append(loc)

        final_locations: list[Location] = []
        for loc in locations:
            if match := LOCATION_REGEX.match(loc):
                campus = match.group("campus")
                building = match.group("building")
                floor = match.group("floor")
                room = match.group("room")

                final_locations.append(
                    cls(campus=campus, building=building, floor=floor, room=room)
                )

        if final_locations:
            return final_locations

        return [cls(campus="", building="", floor="", room="", original=location)]

    def __str__(self) -> str:
        return f"{self.campus}.{self.building}{self.floor}{self.room}"

    def pretty_string(self, include_original: bool = False) -> str:
        building_name = BUILDINGS[self.campus].get(self.building, "[unknown]")
        return (
            f"{self.floor}.{self.room}, "
            f"{building_name} ({self.building}), "
            f"{CAMPUSES[self.campus]} ({self.campus})"
            + (f", ({self!s})" if include_original else "")
        )


class InvalidCodeError(Exception):
    """Invalid code error."""

    code: str
    """The offending code."""

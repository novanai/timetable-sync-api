import asyncio
import datetime
import logging
import typing
from uuid import UUID

import aiohttp
import msgspec
from rapidfuzz import fuzz, process
from rapidfuzz import utils as fuzz_utils

from timetable import __version__, models, utils

if typing.TYPE_CHECKING:
    from timetable import cache as cache_

logger = logging.getLogger(__name__)

BASE_URL = "https://scientia-eu-v4-api-d1-03.azurewebsites.net/api/Public"
INSTITUTION_IDENTITY = "a1fdee6b-68eb-47b8-b2ac-a4c60c8e6177"


def json_serialize(obj: typing.Any) -> str:
    return msgspec.json.encode(obj).decode("utf-8")


class API:
    def __init__(self, valkey_client: "cache_.ValkeyCache") -> None:
        self._cache = valkey_client
        self._session: aiohttp.ClientSession | None = None

    @property
    def cache(self) -> "cache_.ValkeyCache":
        """The Valkey client to use for caching."""
        return self._cache

    @property
    def session(self) -> aiohttp.ClientSession:
        """The `aiohttp.ClientSession` to use for API requests."""
        if not self._session:
            self._session = aiohttp.ClientSession(json_serialize=json_serialize)
        return self._session

    async def _fetch_data(
        self,
        path: str,
        params: dict[str, str] | None = None,
        json_data: dict[str, typing.Any] | None = None,
    ) -> typing.Any:
        """Get data from the api.

        Parameters
        ----------
        path : str
            The API path.
        params : dict[str, str] | None, default None
            Request parameters.
        json_data: dict[str, Any] | None, default None
            Request data.

        Returns
        -------
        Any
            The fetched data.
        """
        retries = 0
        while True:
            async with self.session.request(
                "POST",
                f"{BASE_URL}/{path}",
                params=params,
                headers={
                    "Authorization": "Anonymous",
                    "Content-type": "application/json",
                    "User-Agent": f"TimetableSync/{__version__} (https://timetable.redbrick.dcu.ie)",
                },
                json=json_data,
            ) as res:
                if not res.ok:
                    if retries == 3:
                        logger.error(
                            f"API Error: {res.status} {res.content_type} {(await res.read()).decode()}"
                        )
                        res.raise_for_status()

                    retries += 1
                    await asyncio.sleep(5)
                    continue

                return await res.json(loads=msgspec.json.decode)

    async def fetch_category(
        self,
        category_type: models.CategoryType,
        *,
        query: str | None = None,
        cache: bool | None = None,
        items_type: type[models.CategoryItemT],
    ) -> models.Category[models.CategoryItemT]:
        """Fetch a category.

        Parameters
        ----------
        category_type : models.CategoryType
            The category type.
        query : str | None, default None
            A full or partial course, module or location code to search for.
        cache : bool, default True
            Whether to cache the category.
        items_type : list[models.CategoryItemT]
            The item type to return with the category.

        Returns
        -------
        models.Category
            The fetched category. This is not guaranteed to contain any items.

        Note
        ----
        If query is specified, the category will not be cached.
        """
        if cache is None:
            cache = True

        results: list[dict[str, typing.Any]] = []

        # NOTE: there is an "itemsPerPage" param but it does nothing,
        # every page is always 20 items
        params: dict[str, str] = {
            "pageNumber": "1",
            "query": query.strip() if query else "",
        }

        data = await self._fetch_data(
            f"CategoryTypes/{category_type.value}/Categories/FilterWithCache/{INSTITUTION_IDENTITY}",
            params=params,
        )
        total_pages = data["TotalPages"]
        results.extend(data["Results"])
        count = data["Count"]

        if total_pages > 1:
            data = await asyncio.gather(
                *(
                    self._fetch_data(
                        f"CategoryTypes/{category_type.value}/Categories/FilterWithCache/{INSTITUTION_IDENTITY}",
                        params={
                            "pageNumber": str(i),
                            "query": query.strip() if query else "",
                        },
                    )
                    for i in range(2, total_pages + 1)
                ),
                return_exceptions=True,
            )
            for d in data:
                if isinstance(d, BaseException):
                    raise d

                results.extend(d["Results"])

        final_data = {"Results": results, "Count": count}
        category = models.Category[models.CategoryItem].from_payload(final_data)

        if (query is None or not query.strip()) and cache:
            await self.cache.set_category(
                category_type,
                category,
            )

        # NOTE: pyright complains about this because of the way I manually set
        # the item type but don't worry - it is correct and pyright can shut up
        if items_type is models.BasicCategoryItem:
            return models.Category(  # pyright: ignore[reportReturnType]
                items=[
                    models.BasicCategoryItem(name=item.name, identity=item.identity)
                    for item in category.items
                ],
                count=category.count,
            )

        return category  # pyright: ignore[reportReturnType]

    async def get_category(
        self,
        category_type: models.CategoryType,
        *,
        query: str | None = None,
        limit: int | None = None,
        items_type: type[models.CategoryItemT],
    ) -> models.Category[models.CategoryItemT] | None:
        """Get a category from the cache.

        Parameters
        ----------
        category_type : models.CategoryType
            The category type.
        query : str | None, default None
            A full or partial course, module or location code to search for.
        limit : int | None, default None
            The maximum number of category items to include when searching for `query`. If `None` will
            include all matching items. Ignored if no query is provided.
        items_type : list[models.CategoryItemT]
            The item type to return with the category.

        Returns
        -------
        models.Category
            If the category was cached and is not outdated.
        None
            If the category was not cached or is outdated.
        """
        category = await self.cache.get_category(category_type, items_type)
        if category is None:
            return None

        if query and query.strip():
            filtered = self._filter_category_items_for(category.items, query, limit)
            return models.Category(
                filtered,
                len(filtered),
            )

        return category

    def _filter_category_items_for(
        self,
        category_items: list[models.CategoryItemT],
        query: str,
        limit: int | None,
    ) -> list[models.CategoryItemT]:
        """Filter category items for `query`, only returning items with a >80% match.

        Parameters
        ----------
        category_items : list[models.CategoryItemT]
            The category items to filter.
        query : str
            The query to filter for. Checks against the category's name and code.
        limit : int | None
            The maximum number of category items to include when searching for `query`.
            If `None` will include all matching items.

        Returns
        -------
        list[models.CategoryItemT]
            The items which matched the search query with a >80% match,
            sorted from highest match to lowest.
        """
        names = [item.name for item in category_items]

        matches = process.extract(
            query,
            names,
            scorer=fuzz.partial_ratio,
            processor=fuzz_utils.default_process,
            limit=limit,
            score_cutoff=80,
        )

        return [category_items[idx] for _, _, idx in matches]

    async def fetch_category_item(
        self,
        category_type: models.CategoryType,
        item_identity: UUID,
        cache: bool | None = None,
    ) -> models.CategoryItem:
        """Fetch a category item from the api.

        Parameters
        ----------
        category_type : models.CategoryType
            The type of category type of the item.
        item_identity : UUID
            The identity of the item.
        cache : bool, default True
            Whether to cache the timetables.
        """
        if cache is None:
            cache = True

        data = await self._fetch_data(
            f"CategoryTypes/Categories/Filter/{INSTITUTION_IDENTITY}",
            json_data={
                "CategoryTypesWithIdentities": [
                    {
                        "CategoryTypeIdentity": category_type,
                        "CategoryIdentities": [item_identity],
                    },
                ]
            },
        )

        if not data:
            raise models.InvalidCodeError(item_identity)

        item = models.CategoryItem.from_payload(data[0])

        if cache:
            await self.cache.set_category_item(item)

        return item

    async def get_category_item(
        self,
        item_identity: UUID,
    ) -> models.CategoryItem | None:
        """Get a category item from the cache.

        Parameters
        ----------
        item_identity : UUID
            The identity of the item.
        """
        return await self.cache.get_category_item(item_identity)

    async def fetch_category_items_timetables(
        self,
        category_type: models.CategoryType,
        item_identities: list[UUID],
        start: datetime.datetime | None = None,
        end: datetime.datetime | None = None,
        cache: bool | None = None,
    ) -> list[models.CategoryItemTimetable]:
        """Fetch the timetable for item_identities belonging to category_type.

        Parameters
        ----------
        category_type : models.CategoryType
            The type of category to get timetables in.
        item_identities : list[UUID]
            The identities of the items to get timetables for.
        start : datetime.datetime | None, default Aug 1 of the current academic year
            The start date/time of the timetable.
        end : datetime.datetime | None, default May 1 of the current academic year
            The end date/time of the timetable.
        cache : bool, default True
            Whether to cache the timetables.

        Returns
        -------
        list[models.CategoryItemTimetable]
            The requested timetables.
        """
        if cache is None:
            cache = True

        start_default, end_default = utils.default_year_start_end_dates()
        start, end = utils.calc_start_end_range(start, end)

        data = await self._fetch_data(
            f"CategoryTypes/Categories/Events/Filter/{INSTITUTION_IDENTITY}",
            params={
                "startRange": f"{start_default.isoformat()}Z",
                "endRange": f"{end_default.isoformat()}Z",
            },
            json_data={
                "ViewOptions": {
                    "Days": [
                        {"DayOfWeek": 1},
                        {"DayOfWeek": 2},
                        {"DayOfWeek": 3},
                        {"DayOfWeek": 4},
                        {"DayOfWeek": 5},
                        {"DayOfWeek": 6},
                    ],
                },
                "CategoryTypesWithIdentities": [
                    {
                        "CategoryTypeIdentity": category_type,
                        "CategoryIdentities": item_identities,
                    }
                ],
            },
        )

        timetables: list[models.CategoryItemTimetable] = []
        for timetable_data in data["CategoryEvents"]:
            timetable = models.CategoryItemTimetable.from_payload(timetable_data)

            if cache:
                await self.cache.set_category_item_timetable(timetable)

            timetable.events = list(
                filter(lambda e: start <= e.start <= end, timetable.events)
            )
            timetables.append(timetable)

        return timetables

    async def get_category_item_timetable(
        self,
        item_identity: UUID,
        start: datetime.datetime | None = None,
        end: datetime.datetime | None = None,
    ) -> models.CategoryItemTimetable | None:
        """Get the timetable for item_identity.

        Parameters
        ----------
        item_identity : UUID
            The identity of the item to get the timetable for.
        start : datetime.datetime | None, default Aug 1 of the current academic year
            The start date/time of the timetable.
        end : datetime.datetime | None, default May 1 of the current academic year
            The end date/time of the timetable.

        Returns
        -------
        models.CategoryItemTimetable
            If the timetable was cached and is not outdated.
        None
            If the timetable was not cached or is outdated.
        """
        timetable = await self.cache.get_category_item_timetable(item_identity)
        if timetable is None:
            return None

        if start or end:
            start, end = utils.calc_start_end_range(start, end)
            timetable.events = list(
                filter(lambda e: start <= e.start <= end, timetable.events)
            )

        return timetable

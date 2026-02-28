import datetime
import typing
from uuid import UUID

import msgspec
from glide import (
    ExpirySet,
    ExpiryType,
    GlideClient,
    GlideClientConfiguration,
    NodeAddress,
)

from timetable import cns, models

T = typing.TypeVar("T", bound=msgspec.Struct)


class ValkeyCache:
    """A simple caching implementation using Valkey."""

    def __init__(self, client: GlideClient) -> None:
        self.client = client

    @classmethod
    async def create(cls, host: str, port: int) -> typing.Self:
        addresses = [NodeAddress(host, port)]
        config = GlideClientConfiguration(addresses, request_timeout=500)
        client = await GlideClient.create(config)

        return cls(client)

    async def _set(
        self,
        key: str,
        model: T | list[T],
        expires_in: datetime.timedelta | None = None,
    ) -> None:
        """Cache `data` under `key`.

        Parameters
        ----------
        key : str
            A unique identifier of the data being cached.
        model : msgspec.Struct
            The data model to cache.
        expires_in : datetime.timedelta | None, default: 1 day
            The time this data will expire in.
        """
        if expires_in is None:
            expires_in = datetime.timedelta(days=1)

        await self.client.set(
            key,
            msgspec.msgpack.encode(model),
            expiry=ExpirySet(ExpiryType.SEC, expires_in),
        )

    @typing.overload
    async def _get(self, key: str, model_type: type[T]) -> T | None: ...

    @typing.overload
    async def _get(self, key: str, model_type: type[list[T]]) -> list[T] | None: ...

    async def _get(
        self, key: str, model_type: type[T] | type[list[T]]
    ) -> T | list[T] | None:
        """Get data stored under `key` from the cache.

        Parameters
        ----------
        key : str
            The unique key the data is stored under.
        model_type : type[PayloadModel]
            The type of the model.
        Returns
        -------
        PayloadModel
            The data, if found.
        None
            If the data was not found.
        """
        data = await self.client.get(key)
        return (
            msgspec.msgpack.decode(data, type=model_type) if data is not None else None
        )

    async def set_category(
        self,
        category_type: models.CategoryType,
        category: models.Category[models.CategoryItem],
    ) -> None:
        await self._set(f"category:{category_type.value}", category)

    async def get_category(
        self, category_type: models.CategoryType, model_type: type[models.CategoryItemT]
    ) -> models.Category[models.CategoryItemT] | None:
        return await self._get(
            f"category:{category_type.value}", models.Category[model_type]
        )

    async def set_category_item(self, item: models.CategoryItem) -> None:
        await self._set(f"item:{item.identity}", item)

    async def get_category_item(self, item_id: UUID) -> models.CategoryItem | None:
        return await self._get(f"item:{item_id}", models.CategoryItem)

    async def set_category_item_timetable(
        self, timetable: models.CategoryItemTimetable
    ) -> None:
        await self._set(
            f"timetable:{timetable.identity}",
            timetable,
            expires_in=datetime.timedelta(hours=12),
        )

    async def get_category_item_timetable(
        self, item_id: UUID
    ) -> models.CategoryItemTimetable | None:
        return await self._get(f"timetable:{item_id}", models.CategoryItemTimetable)

    async def set_cns_group_items(
        self, group_type: cns.GroupType, group_items: list[cns.ClubSoc]
    ) -> None:
        await self._set(
            f"cns:{group_type.value}",
            group_items,
        )

    async def get_cns_group_items(
        self, group_type: cns.GroupType
    ) -> list[cns.ClubSoc] | None:
        return await self._get(
            f"cns:{group_type.value}",
            list[cns.ClubSoc],
        )

    async def set_cns_group_item(self, item: cns.ClubSoc) -> None:
        await self._set(f"cns:item:{item.id}", item)

    async def get_cns_group_item(
        self,
        identity: str,
    ) -> cns.ClubSoc | None:
        return await self._get(
            f"cns:item:{identity}",
            cns.ClubSoc,
        )

    async def set_cns_item_events(
        self, identity: str, events: list[cns.Activity | cns.Event | cns.Fixture]
    ) -> None:
        await self._set(
            f"events:{identity}",
            events,
        )

    async def get_cns_item_events(
        self, identity: str
    ) -> list[cns.Activity | cns.Event | cns.Fixture] | None:
        return await self._get(
            f"events:{identity}",
            list[cns.Activity | cns.Event | cns.Fixture],
        )

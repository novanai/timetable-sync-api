import datetime
import typing

import orjson
from glide import GlideClient, GlideClientConfiguration, NodeAddress


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

    async def set(
        self,
        key: str,
        data: dict[str, typing.Any],
        expires_in: datetime.timedelta,
    ) -> None:
        """Cache `data` under `key`.

        Parameters
        ----------
        key : str
            A unique identifier of the data being cached.
        data : dict[str, typing.Any]
            The data to cache.
        """
        await self.client.set(key, orjson.dumps(data))
        await self.client.expire(key, int(expires_in.total_seconds()))

    async def get(self, key: str) -> typing.Any | None:
        """Get data stored under `key` from the cache.

        Parameters
        ----------
        key : str
            The unique key the data is stored under.

        Returns
        -------
        dict[str, typing.Any]
            The data, if found.
        None
            If the data was not found.
        """
        data = await self.client.get(key)
        return orjson.loads(data) if data is not None else None

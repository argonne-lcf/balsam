import json
import logging
from typing import Any, Optional

import aredis  # type: ignore
import redis
from fastapi.encoders import jsonable_encoder

from balsam.server import settings

logger = logging.getLogger(__name__)


class _PubSub:
    """
    Sync Publisher and Async Subscriber (for Websockets)
    """

    def __init__(self) -> None:
        self._redis: Optional[redis.Redis[str]] = None
        self._async_redis = None
        self._has_warned = False

    @property
    def r(self) -> "redis.Redis[str]":
        if self._redis is None:
            self._redis = redis.Redis(**settings.redis_params)
        return self._redis

    @property
    def async_r(self) -> aredis.StrictRedis:
        if self._async_redis is None:
            self._async_redis = aredis.StrictRedis(**settings.redis_params)
        return self._async_redis

    @staticmethod
    def get_topic(user_id: int) -> str:
        return f"user-{user_id}"

    async def subscribe(self, user_id: int) -> "aredis.PubSub":
        p = self.async_r.pubsub(ignore_subscribe_messages=True)
        await p.subscribe(self.get_topic(user_id))
        return p

    def publish(self, user_id: int, action: str, type: str, data: Any) -> None:
        msg = {"action": action, "type": type, "data": data}
        try:
            self.r.publish(self.get_topic(user_id), json.dumps(jsonable_encoder(msg)))
        except redis.exceptions.ConnectionError as e:
            if not self._has_warned:
                logger.warning(f"Redis connection failed!\n{e}\n Proceeding without Redis pub/sub.")
                self._has_warned = True


pubsub = _PubSub()

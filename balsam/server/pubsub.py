from balsam.server import settings
from fastapi.encoders import jsonable_encoder
import redis
import aredis
import json


class _PubSub:
    """
    Sync Publisher and Async Subscriber (for Websockets)
    """

    def __init__(self):
        self._redis = None
        self._async_redis = None

    @property
    def r(self):
        if self._redis is None:
            self._redis = redis.Redis(**settings.redis_params)
        return self._redis

    @property
    def async_r(self):
        if self._async_redis is None:
            self._async_redis = aredis.StrictRedis(**settings.redis_params)
        return self._async_redis

    @staticmethod
    def get_topic(user_id):
        return f"user-{user_id}"

    async def subscribe(self, user_id):
        p = self.async_r.pubsub(ignore_subscribe_messages=True)
        await p.subscribe(self.get_topic(user_id))
        return p

    def publish(self, user_id, action, type, data):
        msg = {"action": action, "type": type, "data": data}
        self.r.publish(self.get_topic(user_id), json.dumps(jsonable_encoder(msg)))


pubsub = _PubSub()

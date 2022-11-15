import json
from abc import abstractmethod, ABC

from models.state import StateModel
from storage_clients.redis_client import RedisClient


class BaseStorage(ABC):
    @abstractmethod
    def is_state_exists(self, key: str) -> None:
        """Проверить наличие состояния в постоянное хранилище"""
        pass

    @abstractmethod
    def save_state(self, key: str, value: object) -> None:
        """Сохранить состояние в постоянное хранилище"""
        pass

    @abstractmethod
    def retrieve_state(self, key: str) -> dict | None:
        """Загрузить состояние локально из постоянного хранилища"""
        pass


class RedisStorage(BaseStorage):
    def __init__(self, redis_adapter: RedisClient):
        self.redis_adapter = redis_adapter

    def is_state_exists(self, key: str) -> int:
        return self.redis_adapter.exists(key)

    def save_state(self, key: str, value: object) -> None:
        self.redis_adapter.set(key, json.dumps(value))

    def retrieve_state(self, key: str) -> dict | None:
        result = self.redis_adapter.get(key)

        if result:
            return json.loads(result)

        return result


class State:
    def __init__(self, storage: BaseStorage, key: str):
        self.storage = storage
        self.key = key

    def exists(self):
        """Проверить наличие определённого ключа"""
        return self.storage.is_state_exists(self.key)

    def set(self, value: str) -> None:
        """Установить состояние для определённого ключа"""
        self.storage.save_state(self.key, {"updated_at": value})

    def get(self) -> StateModel | None:
        """Получить состояние по определённому ключу"""
        result = self.storage.retrieve_state(self.key)

        if result:
            result = StateModel(**result)

        return result

import time
from functools import wraps
from typing import Type, Any, Callable

from storage_clients.base_client import AbstractClientInterface
from helpers.logger import LoggerFactory

logger = LoggerFactory().get_logger()


def reconnect(func: Callable) -> Any:
    """ Reconnect to client on failure."""
    @wraps(func)
    def wrapper(storage: AbstractClientInterface, *args, **kwargs):
        if not storage.is_connected:
            logger.warning("Lost connection to client: `%r`. Trying to establish new connection...", storage)
            storage.reconnect()

        return func(storage, *args, **kwargs)

    return wrapper


def backoff(
        exceptions: Type[object] | tuple[Type[BaseException]] | Any,
        start_sleep_time: float = 0.1,
        factor: float | int = 2,
        border_sleep_time: float = 10
) -> Any:
    """
    Функция для повторного выполнения функции через некоторое время, если возникла ошибка.
    Использует наивный экспоненциальный рост времени повтора (factor) до граничного времени ожидания (border_sleep_time)

    Формула:
        t = start_sleep_time * 2^(n) if t < border_sleep_time
        t = border_sleep_time if t >= border_sleep_time
    :param exceptions: ожидаемое исключение
    :param start_sleep_time: начальное время повтора
    :param factor: во сколько раз нужно увеличить время ожидания
    :param border_sleep_time: граничное время ожидания
    :return: результат выполнения функции

    """

    def func_wrapper(func: Callable):
        @wraps(func)
        def inner(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except exceptions as e:
                logger.error(
                    "Call: `%s` failed with `%s`. Going for backoff...",
                    func.__name__, str(e)
                )
                t = start_sleep_time
                n = 0
                while True:
                    try:
                        return func(*args, **kwargs)
                    except exceptions as e:
                        if t < border_sleep_time:
                            # not to calc exp to inf
                            t = t * factor ** n
                            n += 1

                        sleep = min(t, border_sleep_time)
                        logger.error(
                            "Call: `%s` failed with `%s` Trying to retry after `%s`...",
                            func.__name__, type(e), sleep
                        )
                        time.sleep(sleep)

        return inner

    return func_wrapper

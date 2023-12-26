import functools
import time


def backoff(tries: int, sleep: int):
    """
    Декоратор для повторных попыток выполнить код
    :param tries: количество попыток
    :param sleep: время ожидания до следующей попытки
    :return:
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            n_tries = 0
            while n_tries < tries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    n_tries += 1
                    if n_tries < tries:
                        time.sleep(sleep)
                    else:
                        raise e

        return wrapper

    return decorator

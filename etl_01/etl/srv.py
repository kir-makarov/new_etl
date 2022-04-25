import logging
import time
from datetime import datetime
from functools import wraps

log = logging.getLogger('ConnectLog')


def backoff(start_sleep_time=0.1, factor=2, border_sleep_time=3):
    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            t = start_sleep_time
            count = 0
            while True:
                try:
                    return func(*args, **kwargs)
                except Exception as err:
                    time.sleep(t)
                    if t >= border_sleep_time:
                        t = border_sleep_time
                    if t < border_sleep_time:
                        t *= factor
                    log.error(f'{datetime.now()}\n\n{err}\n\nОсуществляется попытка подключения {count}')
                    count += 1
                    continue
                finally:
                    if count == 10:
                        log.info(f'{datetime.now()}\n\nПодключение прервано, так как иcчерпаны попытки')
                        break

        return inner

    return func_wrapper

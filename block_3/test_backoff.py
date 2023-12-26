import time

from block_3.utils import backoff

counter = 0
last_time = None
timing_ok = True


def test_backoff():
    global timing_ok
    tries = 3
    sleep_time = 1

    def failing_func(x: int) -> int:
        global counter, last_time, timing_ok
        if counter < tries - 1:
            if last_time is not None:
                if time.time() - last_time < sleep_time:
                    timing_ok = False

            last_time = time.time()
            counter += 1
            raise ValueError("failing on counter")
        else:
            return x

    @backoff(tries=tries, sleep=sleep_time)
    def func1(x: int) -> int:
        return failing_func(x)

    @backoff(tries=tries - 1, sleep=sleep_time)
    def func2(x: int) -> int:
        return failing_func(x)

    # func1 должен вернуть то же число и тайминги не должны быть меньше, чем заданное число секунда
    assert func1(42) == 42 and timing_ok
    # func2 должен упасть
    try:
        func2(42)
        raise AssertionError("test should raise exception")
    except:
        pass

    print("Backoff works!")


if __name__ == '__main__':
    test_backoff()

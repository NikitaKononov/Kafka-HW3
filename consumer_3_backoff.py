from kafka import KafkaConsumer
import time
from functools import wraps
import random


# декоратор реализация бекофа
def backoff(tries, sleep):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            ntries = tries
            while ntries > 0:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    print(f"Error: {e}, Retrying in {sleep} seconds...")
                    time.sleep(sleep)
                    ntries -= 1

            raise Exception(f"FATAL: Failed to execute {func.__name__} after {tries} tries")

        return wrapper

    return decorator


@backoff(tries=10, sleep=5)
def message_handler(value) -> None:
    # типа что-то делали и получаем экспешен с некоторой вероятностью (имитация ошибки)
    if random.random() < 0.75:
        raise Exception("Failed to do something")
    print(value)


def create_consumer():
    print("Connecting to Kafka brokers")
    consumer = KafkaConsumer("kononov",
                             group_id='kononov_group1',
                             bootstrap_servers='localhost:29092',
                             auto_offset_reset='earliest',
                             enable_auto_commit=True)

    for message in consumer:
        # send to http get (rest api) to get response
        # save to db message (kafka) + external
        message_handler(message)
        print(message)


if __name__ == '__main__':
    create_consumer()

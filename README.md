# Kafka-HW3
kafka+flink homework

### я прикрепил свое дз по хадупу в качестве сабмодуля, чтобы не пушить сюда то же самое 
#### (оттуда использовал композник для подъема хадупа)

# Блок 1 (Flink checkpoint )

Композ есть

Запуск прост:
```
# в одном терминале запускаем продюсера
python3 producer_1.py
```

```
# в другом терминале читаем из топика kononov
python3 consumer_1.py
```

```
# в третьем терминале читаем из топика kononov processed
python3 consumer_2.py
```

```
# в четвертом (xD) терминале запуск джобы флинка с чекпоинтом в локалдир
docker-compose exec jobmanager ./bin/flink run -py /opt/pyflink/device_job_localdir.py -d
# или с чекпоинтом в хадуп
docker-compose exec jobmanager ./bin/flink run -py /opt/pyflink/device_job_hadoop.py -d
```

```
# Топики надо предварительно создать
docker-compose exec kafka kafka-topics.sh --bootstrap-server kafka:9092 --create --topic kononov --partitions 1 --replication-factor 1
docker-compose exec kafka kafka-topics.sh --bootstrap-server kafka:9092 --create --topic kononov_processed --partitions 1 --replication-factor 1
```

можно и в пайчарм просто запустить эти 3 файла

## директория localdir чекпоинт флинка

![img.png](misc/localdir.png)

## директория hadoop чекпоинт флинка


# Блок 2 (Flink Window)


# Блок 3 (Kafka backoff)

Код в consumer_3_backoff.py

Запуск прост:
```
# в одном терминале
python3 producer_1.py
```

```
# в другом терминале
python3 consumer_3_backoff.py
```

можно и в пайчарм просто запустить эти 2 файла

Приложу скрин кода сюда, всяко удобнее, чем вам лазить по файлам:
![img.png](misc/backoff.png)
from pyflink.common import SimpleStringSchema
from pyflink.common import Time
from pyflink.common.typeinfo import Types, RowTypeInfo
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import DeliveryGuarantee
from pyflink.datastream.connectors.kafka import KafkaSource, \
    KafkaOffsetsInitializer, KafkaSink, KafkaRecordSerializationSchema
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.datastream.functions import ReduceFunction
from pyflink.datastream.window import EventTimeSessionWindows


class MaxTemperatureFunction(ReduceFunction):
    def reduce(self, value1, value2):
        # Return max temperature between two values
        return (value1[0], max(value1[1], value2[1]), value1[2])


def python_data_stream_example():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    type_info: RowTypeInfo = Types.ROW_NAMED(['device_id', 'temperature', 'execution_time'],
                                             [Types.LONG(), Types.DOUBLE(), Types.INT()])

    json_row_schema = JsonRowDeserializationSchema.builder().type_info(type_info).build()

    source = KafkaSource.builder() \
        .set_bootstrap_servers('kafka:9092') \
        .set_topics('kononov') \
        .set_group_id('pyflink-e2e-source') \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(json_row_schema) \
        .build()

    sink = KafkaSink.builder() \
        .set_bootstrap_servers('kafka:9092') \
        .set_record_serializer(KafkaRecordSerializationSchema.builder()
                               .set_topic('kononov_processed')
                               .set_value_serialization_schema(SimpleStringSchema())
                               .build()
                               ) \
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
        .build()

    env.from_source(source=source, watermark_strategy=WatermarkStrategy.no_watermarks(),
                    source_name="Kafka Source") \
        .key_by(lambda value: value[0]) \
        .window(EventTimeSessionWindows.withGap(Time.minutes(1))) \
        .reduce(MaxTemperatureFunction()) \
        .map(lambda value: str({"device_id": value[0], "max_temperature": value[1], "execution_time": value[2]}),
             Types.STRING()) \
        .sink_to(sink)

    env.execute("Devices preprocessing")


if __name__ == '__main__':
    python_data_stream_example()

from pyflink.common import SimpleStringSchema, Time
from pyflink.common.typeinfo import Types, RowTypeInfo
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.datastream.connectors import DeliveryGuarantee
from pyflink.datastream.connectors.kafka import KafkaSource, \
    KafkaOffsetsInitializer, KafkaSink, KafkaRecordSerializationSchema
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.datastream.functions import MapFunction, ReduceFunction
from pyflink.datastream.window import TumblingProcessingTimeWindows, SlidingProcessingTimeWindows, \
    ProcessingTimeSessionWindows


def python_data_stream_example():
    env = StreamExecutionEnvironment.get_execution_environment()
    # Set the parallelism to be one to make sure that all data including fired timer and normal data
    # are processed by the same worker and the collected result would be in order which is good for
    # assertion.
    env.set_parallelism(1)
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)

    type_info: RowTypeInfo = Types.ROW_NAMED(['device_id', 'temperature', 'execution_time'],
                                             [Types.LONG(), Types.DOUBLE(), Types.INT()])

    json_row_schema = JsonRowDeserializationSchema.builder().type_info(type_info).build()

    source = KafkaSource.builder() \
        .set_bootstrap_servers('kafka:9092') \
        .set_topics('itmo2023') \
        .set_group_id('pyflink-e2e-source') \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(json_row_schema) \
        .build()

    tumbling_window_sink = KafkaSink.builder() \
        .set_bootstrap_servers('kafka:9092') \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic('itmo2023-processed-tumbling-window')
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        ) \
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
        .build()

    sliding_window_sink = KafkaSink.builder() \
        .set_bootstrap_servers('kafka:9092') \
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic('itmo2023-processed-sliding-window')
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        ) \
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
        .build()

    session_window_sink = KafkaSink.builder() \
        .set_bootstrap_servers('kafka:9092') \
        .set_record_serializer(
        KafkaRecordSerializationSchema.builder()
        .set_topic('itmo2023-processed-session-window')
        .set_value_serialization_schema(SimpleStringSchema())
        .build()
    ) \
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE) \
        .build()

    ds = env.from_source(source, WatermarkStrategy.no_watermarks(), "Kafka Source")
    # Tumbling window
    ds.window_all(TumblingProcessingTimeWindows.of(Time.seconds(10))) \
        .reduce(MaxTemperatureReducer()) \
        .map(RowStringTransform(), Types.STRING()) \
        .sink_to(tumbling_window_sink)
    # Sliding window
    ds.window_all(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5))) \
        .reduce(MaxTemperatureReducer()) \
        .map(RowStringTransform(), Types.STRING()) \
        .sink_to(sliding_window_sink)
    # Session window
    ds.window_all(ProcessingTimeSessionWindows.with_gap(Time.seconds(10))) \
        .reduce(MaxTemperatureReducer()) \
        .map(RowStringTransform(), Types.STRING()) \
        .sink_to(session_window_sink)
    env.execute_async("Max temperature in window")


class MaxTemperatureReducer(ReduceFunction):
    def reduce(self, v1, v2):
        return max(v1, v2, key=lambda x: x[1])


class RowStringTransform(MapFunction):
    def map(self, value):
        device_id, temperature, execution_time = value
        return str({"device_id": device_id, "temperature": temperature, "execution_time": execution_time})


if __name__ == '__main__':
    python_data_stream_example()

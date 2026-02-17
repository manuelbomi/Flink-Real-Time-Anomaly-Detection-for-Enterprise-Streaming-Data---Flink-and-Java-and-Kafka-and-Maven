package com.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class AnomalyDetectionJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "flink-anomaly-group");

        FlinkKafkaConsumer<String> consumer =
                new FlinkKafkaConsumer<>(
                        "sensor-data_2",
                        new SimpleStringSchema(),
                        props
                );

        DataStream<String> stream = env.addSource(consumer);

        DataStream<SensorReading> parsed = stream.map(new ParseEvent());

        DataStream<String> anomalies = parsed
                .keyBy(r -> r.sensorId)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .apply(new AnomalyDetector());

        anomalies.print();

        env.execute("Flink Anomaly Detection (Java)");
    }

    // -------------------------

    public static class ParseEvent implements MapFunction<String, SensorReading> {
        @Override
        public SensorReading map(String value) {
            String[] parts = value.split(",");
            return new SensorReading(parts[0], Double.parseDouble(parts[1]));
        }
    }

    // -------------------------

    public static class AnomalyDetector
            implements WindowFunction<SensorReading, String, String, org.apache.flink.streaming.api.windowing.windows.TimeWindow> {

        @Override
        public void apply(String key,
                          org.apache.flink.streaming.api.windowing.windows.TimeWindow window,
                          Iterable<SensorReading> input,
                          Collector<String> out) {

            double sum = 0;
            int count = 0;

            for (SensorReading r : input) {
                sum += r.value;
                count++;
            }

            if (count == 0) return;

            double avg = sum / count;

            for (SensorReading r : input) {
                if (Math.abs(r.value - avg) > 50) {
                    out.collect("ANOMALY → " + key +
                            ": value=" + r.value +
                            ", avg=" + avg);
                }
            }
        }
    }

    // -------------------------

    public static class SensorReading {
        public String sensorId;
        public double value;

        public SensorReading() {}

        public SensorReading(String sensorId, double value) {
            this.sensorId = sensorId;
            this.value = value;
        }
    }
}

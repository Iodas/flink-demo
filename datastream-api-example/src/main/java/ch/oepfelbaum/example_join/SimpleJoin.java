package ch.oepfelbaum.example_join;

import ch.oepfelbaum.example_join.deserializer.AssetDeserializer;
import ch.oepfelbaum.example_join.deserializer.PositionDeserializer;
import ch.oepfelbaum.example_join.model.Asset;
import ch.oepfelbaum.example_join.model.Position;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;

import java.time.Duration;
import java.time.Instant;

public class SimpleJoin {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment().enableCheckpointing().setParallelism(3);

        // SOURCES
        final KafkaSource<Asset> assetSource = KafkaSource.<Asset>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("oepfelbaum-asset")
                .setGroupId("asset-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new AssetDeserializer())
                .build();

        final KafkaSource<Position> positionSource = KafkaSource.<Position>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("oepfelbaum-position")
                .setGroupId("position-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new PositionDeserializer())
                .build();

        // WATERMARK STRATEGY - Processing time
        WatermarkStrategy<Asset> assetWatermarkStrategy = WatermarkStrategy
                .<Asset>forMonotonousTimestamps()
                .withIdleness(Duration.ofSeconds(1))
                .withTimestampAssigner(new SerializableTimestampAssigner<Asset>() {
                    @Override
                    public long extractTimestamp(Asset element, long recordTimestamp) {
                        return Instant.now().toEpochMilli();
                    }
                });

        WatermarkStrategy<Position> positionWatermarkStrategy = WatermarkStrategy
                .<Position>forMonotonousTimestamps()
                .withIdleness(Duration.ofSeconds(1))
                .withTimestampAssigner(new SerializableTimestampAssigner<Position>() {
                    @Override
                    public long extractTimestamp(Position element, long recordTimestamp) {
                        return Instant.now().toEpochMilli();
                    }
                });

        // INITIALIZE STREAMS
        final SingleOutputStreamOperator<Asset> assetStream = env.fromSource(assetSource,
                        WatermarkStrategy.noWatermarks(), "Asset Source")
                .assignTimestampsAndWatermarks(assetWatermarkStrategy);

        final SingleOutputStreamOperator<Position> positionStream = env.fromSource(positionSource,
                        WatermarkStrategy.noWatermarks(), "Position Source")
                .assignTimestampsAndWatermarks(positionWatermarkStrategy);

        final DataStream<Position> positionWithAssetStream = assetStream
                .keyBy(Asset::getId)
                .connect(positionStream.keyBy(Position::getAssetId))
                .process(new PositionWithAssetJoinFunction());

        final FileSink<Position> positionSink = FileSink
                .forRowFormat(new Path("./output/position"), new SimpleStringEncoder<Position>("UTF-8"))
                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd--HH-mm"))
                .withOutputFileConfig(OutputFileConfig.builder()
                        .withPartSuffix(".csv")
                        .build())
                .build();

        final FileSink<Asset> assetSink = FileSink
                .forRowFormat(new Path("./output/asset"), new SimpleStringEncoder<Asset>("UTF-8"))
                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd--HH-mm"))
                .withOutputFileConfig(OutputFileConfig.builder()
                        .withPartSuffix(".csv")
                        .build())
                .build();

        positionWithAssetStream.sinkTo(positionSink);
        assetStream.sinkTo(assetSink);

        env.execute("Aixigo Flink");
    }
}
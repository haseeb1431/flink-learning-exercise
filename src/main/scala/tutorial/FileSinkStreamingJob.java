package tutorial;

import Models.CabRide;
import Sources.CabEventSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.RollingPolicy;

import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;

import java.util.concurrent.TimeUnit;


import java.util.Properties;

public class FileSinkStreamingJob {

    public static void main(String[] args) throws Exception {

        // Read config file
        String configPath = "config.properties";
        Properties config = new Properties();

        // Read configuration properties
        config.load(FileSinkStreamingJob.class.getClassLoader().getResourceAsStream(configPath));

        String symbol = config.getProperty("symbol");
        long half_life = Long.parseLong(config.getProperty("half_life"));
        int sample_rate = Integer.parseInt(config.getProperty("sample_period"));
        String outputFolder = config.getProperty("output_path");
        String outputPath = String.format("%s/symbol=%s/", outputFolder, symbol);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        KeyedStream keyedStream = env
                .addSource(new CabEventSource())
                .filter(x-> x.driverName().equals("Haseeb"))
                .keyBy(x -> x.driverName());

        keyedStream.addSink(getFileSink()).name("File SInk");
        keyedStream.addSink(new PrintSinkFunction()).name("Print sink");



        env.execute();
    }

    private static StreamingFileSink<CabRide> getFileSink() {

        String basePath = "src/main/resources/";

        RollingPolicy rollingPolicy = DefaultRollingPolicy.builder()
                .withRolloverInterval(TimeUnit.SECONDS.toMillis(10))
                //.withInactivityInterval(TimeUnit.MINUTES.toMillis(1))
                //.withMaxPartSize(1024 * 1024 * 128)
                .build();

        StreamingFileSink<CabRide> sink = StreamingFileSink
                .forRowFormat(new Path(basePath), new SimpleStringEncoder<CabRide>("UTF-8"))
                .withRollingPolicy(rollingPolicy)
                .withBucketAssigner(new DriverNameBucketAssigner())
                .build();

        return sink;
    }

    static class DriverNameBucketAssigner implements BucketAssigner<CabRide, String> {

        @Override
        public String getBucketId(CabRide cabRide, Context context) {
            return "symbol="+cabRide.driverName();
        }

        @Override
        public SimpleVersionedSerializer<String> getSerializer() {
            return SimpleVersionedStringSerializer.INSTANCE;
        }
    }
}


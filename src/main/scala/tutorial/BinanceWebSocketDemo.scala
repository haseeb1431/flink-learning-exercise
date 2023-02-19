package tutorial

import Models.BinanceBookTicker
import Sources.BinanceWebSocketSource
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.core.io.SimpleVersionedSerializer
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction
import org.apache.flink.streaming.api.functions.sink.filesystem.{BucketAssigner, StreamingFileSink}
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.yaml.snakeyaml.Yaml

import java.io.FileInputStream
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import java.time.format.DateTimeFormatter
import java.time.{Duration, LocalDateTime}

object BinanceWebSocketDemo {

  var half_life: Long = _
  var sample_period: Long = _
  var output_path: String = _
  var symbol: String = _

  def main(args: Array[String]): Unit = {

    val confiFilePath = if (args.length > 0) args(0) else "src/main/resources/config.yml"

    val config = readTheConfigFile(confiFilePath)

    // create the stream execution environment with the webUI for monitoring on local
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()
    env.getCheckpointConfig.setCheckpointInterval(config.get("checkPoint_interval").asInstanceOf[Long])

    //val tickerStream: DataStream[BinanceBookTicker] = localFileSource(env, symbol)
    val tickerStream: DataStream[BinanceBookTicker] = liveBinanceSource(env, symbol)

    //calculatle the decay based on the sample rate
    val outputStream: DataStream[(String, Long, Double, Double)] = calculateDecaySum(tickerStream, sample_period)

    val csvFileSink = getStreamingFileSink(output_path)

    outputStream.addSink(csvFileSink).name("symbols CSV sink")
    outputStream.addSink(new PrintSinkFunction[(String, Long, Double, Double)]()).name("print to console sink")

    env.execute("Binance WebSocket Demo")
  }

  def readTheConfigFile(configFilePath: String): java.util.Map[String, Any] = {
    // Read the YAML file
    val input = new FileInputStream(configFilePath)
    val yaml = new Yaml()
    val data = yaml.load(input).asInstanceOf[java.util.Map[String, Any]]

    // Get the properties from the YAML data
    symbol = data.get("symbol").asInstanceOf[String]
    output_path = data.get("output_path").asInstanceOf[String]
    half_life = data.get("half_life").asInstanceOf[Long]
    sample_period = data.get("sample_period").asInstanceOf[Long]

    data
  }

  def calculateDecaySum(tickerStream: DataStream[BinanceBookTicker], sample_rate: Long): DataStream[(String, Long, Double, Double)] = {
    val watermarkStrategy = WatermarkStrategy
      .forBoundedOutOfOrderness[BinanceBookTicker](Duration.ofSeconds(3))
      .withTimestampAssigner(new SerializableTimestampAssigner[BinanceBookTicker] {
        override def extractTimestamp(element: BinanceBookTicker, recordTimestamp: Long): Long = element.eventTime
      })

    // Parse each line of the text file as a JSON object and then assign water mark based on the event time
    val keyedStream: KeyedStream[BinanceBookTicker, String] =
      tickerStream
        .assignTimestampsAndWatermarks(watermarkStrategy)
        .keyBy(_.symbol)


    keyedStream
      .window(TumblingEventTimeWindows.of(Time.milliseconds(sample_rate))) // window of required sample size in millisecond
      .process(new ProcessWindowFunction[BinanceBookTicker, (String, Long, Double, Double), String, TimeWindow] {

        def process(key: String, context: Context, input: Iterable[BinanceBookTicker], out: Collector[(String, Long, Double, Double)]) = {
          var decaySumAsk = 0D
          var decaySumBid = 0D
          var bestAsk = 0D //todo: can price be negative?
          var bestBid = 0D //
          //            //the math is actually pretty simple: take the previous timestamp and subtract the current timestamp.
          //            //divide it by the half life (which is a variable to be set) and multiply by 2. add the result to the price
          //            //and multiply by the previous exponential time decayed sum (which is the result of this function for the previous item)
          var prevRecord: BinanceBookTicker = null

          for (curRecord <- input) {
            //decaySum = ((decaySum *  / half_life) + curRecord.askPrice) * prevRecord.askPrice
            prevRecord = curRecord //TODO: first record will make the di zero because of the time difference
            var di = Math.pow(2, (prevRecord.eventTime - curRecord.eventTime) / half_life)
            decaySumAsk = decaySumAsk * di + curRecord.askPrice
            decaySumBid = decaySumBid * di + curRecord.bidPrice

            if (decaySumAsk > bestAsk) bestAsk = decaySumAsk
            if (decaySumBid > bestAsk) bestBid = decaySumBid

          }
          //output based on the sample
          out.collect((key, context.window.getEnd(), bestBid, bestAsk))
        }
      })


  }

  def calculateDecaySum_OLD(tickerStream: DataStream[BinanceBookTicker]): KeyedStream[BinanceBookTicker, String] = {

    val watermarkStrategy = WatermarkStrategy
      .forBoundedOutOfOrderness[BinanceBookTicker](Duration.ofSeconds(3))
      .withTimestampAssigner(new SerializableTimestampAssigner[BinanceBookTicker] {
        override def extractTimestamp(element: BinanceBookTicker, recordTimestamp: Long): Long = element.eventTime
      })

    // Parse each line of the text file as a JSON object and then assign water mark based on the event time
    val keyedStream: KeyedStream[BinanceBookTicker, String] =
      tickerStream
        .assignTimestampsAndWatermarks(watermarkStrategy)
        .keyBy(_.symbol)

    keyedStream
  }

  def liveBinanceSource(env: StreamExecutionEnvironment, symbol: String): DataStream[BinanceBookTicker] = {
    val source = new BinanceWebSocketSource("", "", symbol)
    env.addSource(source)

  }

  def localFileSource(env: StreamExecutionEnvironment, symbol: String): DataStream[BinanceBookTicker] = {

    val filePath = "src/main/resources/transactions.json"

    // Read the file as a text file
    val parsedStream: DataStream[BinanceBookTicker] =
      env
        .readTextFile(filePath)
        .map(line => BinanceBookTicker.fromString(line))

    parsedStream


    // Print the parsed JSON objects to the console
    //parsedStream.print()


  }

  def getStreamingFileSink(outputFolder: String): StreamingFileSink[(String, Long, Double, Double)] = {


    // write the CSV stream to a file every 60 seconds
    val csvFileSink = StreamingFileSink
      .forRowFormat(new Path(outputFolder), new SimpleStringEncoder[(String, Long, Double, Double)]("UTF-8"))
      .withRollingPolicy(DefaultRollingPolicy.builder().withRolloverInterval(10000L).build())
      .withBucketAssigner(new BucketAssigner[(String, Long, Double, Double), String] {
        override def getBucketId(element: (String, Long, Double, Double), context: BucketAssigner.Context): String = {
          s"symbol=${element._1}"
          //          val pattern = "yyyy-MM-dd_HH"
          //          // Get the current date and time
          //          val now = LocalDateTime.now()
          //
          //          // Format the date and time according to the pattern
          //          val formattedDateTime = now.format(DateTimeFormatter.ofPattern(pattern))
          //
          //          // Return the bucket id based on the formatted date and time
          //          s"symbol=${element._1}/${formattedDateTime}"
        }
        override def getSerializer: SimpleVersionedSerializer[String] = SimpleVersionedStringSerializer.INSTANCE
      })
      .build()

    csvFileSink





    //    val rollingPolicy: RollingPolicy[BinanceBookTicker, String] = DefaultRollingPolicy
    //      .builder()
    //      .withInactivityInterval(10000)
    //      .withMaxPartSize(1024 * 1024 * 128) // Set the maximum file size to 128 MB
    //      .withRolloverInterval(60 * 1000)
    //      .build()

    //    val parqueetSink: StreamingFileSink[BinanceBookTicker] = StreamingFileSink
    //      .forBulkFormat(new Path(outputPath),  ParquetAvroWriters.forReflectRecord(classOf[BinanceBookTicker]))
    //
    //      //.withRollingPolicy(rollingPolicy)
    //      .build()


    //    tickerStream
    //      .print()
    //.addSink(parqueetSink)


    // create a schema for the BinanceBookTicker
    //    val schema = new Schema.Parser().parse(
    //      """{"type":"record","name":"ExampleClass","namespace":"com.example","fields":[{"name":"name","type":"string"},{"name":"age","type":"int"}]}"""
    //    )
    //
    //    // write the dataset to a parquet file on the local file system
    //    processStream
    //
    //      .map(ParquetAvroWriters.forReflectRecord(schema))
    //      .writeAsText("/tmp/example.parquet", WriteMode.OVERWRITE)

    //
  }

}


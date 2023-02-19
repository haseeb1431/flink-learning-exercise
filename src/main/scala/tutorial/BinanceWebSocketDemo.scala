package tutorial

import Models.BinanceBookTicker
import Sources.BinanceWebSocketSource
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.{AggregateFunction, MapFunction, RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.filesystem.{BucketAssigner, StreamingFileSink}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.core.io.SimpleVersionedSerializer
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.types.Nothing

import scala.collection.mutable.ArrayBuffer
//import org.apache.flink.core.fs.Path
//import org.apache.flink.formats.parquet.avro.ParquetAvroWriters
//import org.apache.flink.formats.parquet.protobuf.ParquetProtoWriters
//import org.apache.flink.streaming.api.functions.sink.filesystem.{RollingPolicy, StreamingFileSink}
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._
//import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
//import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy

import java.time.Duration
import java.util.Properties


object BinanceWebSocketDemo {


  var half_life: Long = _
  var sample_rate: Int = _

  def main(args: Array[String]): Unit = {

    //val env = StreamExecutionEnvironment.getExecutionEnvironment
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()

    // Read config file
    val configPath = "config.properties"
    val config = new Properties()

    // Read configuration properties
    config.load(getClass.getClassLoader.getResourceAsStream(configPath))

    half_life = config.getProperty("half_life").toLong
    sample_rate = config.getProperty("sample_period").toInt
    val outputFolder = config.getProperty("output_path")
    val symbol = config.getProperty("symbol")
    //val tickerStream: DataStream[BinanceBookTicker] = localFileSource(env, symbol)
    val tickerStream: DataStream[BinanceBookTicker] = liveBinanceSource(env, symbol)

    val outputStream:DataStream[(String, Long, Double, Double)] = calculateDecaySum(tickerStream)

    val csvFileSink = getStreamingFileSink(outputFolder)

    outputStream.addSink(csvFileSink).name("symbols CSV sink")
    outputStream.addSink(new PrintSinkFunction[(String, Long, Double, Double)]()).name("print to console sink")

    env.execute("Binance WebSocket Demo")
  }

  //TODO: update the code to use window and produce output based on the sample
  //TODO: update api call for multiple symbols

  //TODO: change the config file to YAML

  //TODO: Exactly once and why we cannot - add to the doc
  //TODO: add the instructions to run it as well, flink-submit should work

  def calculateDecaySum(tickerStream: DataStream[BinanceBookTicker]): DataStream[(String, Long, Double, Double)] = {
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

        def process(key: String, context: Context, input: Iterable[BinanceBookTicker], out: Collector[(String,Long, Double, Double)]) = {
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

  def old() = {
    //          //.process(new ProcessWindowFunction[BinanceBookTicker, (Long, Double, Double),String, TimeWindow] {
    //
    //            def process(key: String, context: Context, input: Iterable[BinanceBookTicker], out: Collector[(Long, Double, Double)]) = {
    //              var decaySum = 0D
    //
    ////            //the math is actually pretty simple: take the previous timestamp and subtract the current timestamp.
    ////            //divide it by the half life (which is a variable to be set) and multiply by 2. add the result to the price
    ////            //and multiply by the previous exponential time decayed sum (which is the result of this function for the previous item)
    //
    //              var prevRecord:BinanceBookTicker = null
    //
    //              for (curRecord <- input) {
    //                //decaySum = ((decaySum *  / half_life) + curRecord.askPrice) * prevRecord.askPrice
    //                prevRecord = curRecord //first record will make the di zero because of the time difference
    //                var di =Math.pow(2,(curRecord.eventTime - prevRecord.eventTime)/half_life)
    //                decaySum = decaySum * di + curRecord.askPrice
    //              }
    //
    //              out.collect((1,11,1))
    //
    //            }
    //
    //          })


    //.process(new ProcessWindowFunction<Tuple2<Event, Integer>, String, Integer, TimeWindow>() {})

    //    var processStream =
    //      keyedStream
    //        .window(TumblingEventTimeWindows.of(Time.of(1000))
    //        .flatMap(new RichFlatMapFunction[BinanceBookTicker, (Long, Double, Double)] {
    //
    //          var prevRecordState: ValueState[(Long, Double)] = _
    //
    //          override def flatMap(curRecord: BinanceBookTicker, collector: Collector[(Long, Double, Double)]): Unit = {
    //
    //            // access the state value
    //            val tmpPrevRecordState = prevRecordState.value
    //
    //            // If it hasn't been used before, it will be null
    //            val prevRecord: (Long, Double) = if (tmpPrevRecordState != null) {
    //              tmpPrevRecordState
    //            } else {
    //              (curRecord.eventTime, 1)
    //            }
    //
    //            //the math is actually pretty simple: take the previous timestamp and subtract the current timestamp.
    //            //divide it by the half life (which is a variable to be set) and multiply by 2. add the result to the price
    //            //and multiply by the previous exponential time decayed sum (which is the result of this function for the previous item)
    //            val timeDecaySum = ((2 * (curRecord.eventTime - prevRecord._1) / half_life) + curRecord.askPrice) * prevRecord._2
    //
    //            // update the state
    //            prevRecordState.update((curRecord.eventTime, timeDecaySum))
    //            //s"Diff: ${curRecord.eventTime - prevRecord._1}, TDS: ${timeDecaySum}"
    //
    //            collector.collect((curRecord.eventTime, curRecord.bidPrice, timeDecaySum))
    //          }
    //
    //          override def open(parameters: Configuration): Unit = {
    //            prevRecordState = getRuntimeContext.getState(
    //              new ValueStateDescriptor[(Long, Double)]("prevRecord", createTypeInformation[(Long, Double)])
    //            )
    //          }
    //
    //        })
    //
    //    processStream
  }

  def getStreamingFileSink(outputFolder: String): StreamingFileSink[(String, Long, Double, Double)] ={


    // write the CSV stream to a file every 60 seconds
    val csvFileSink = StreamingFileSink
      .forRowFormat(new Path(outputFolder), new SimpleStringEncoder[(String, Long, Double, Double)]("UTF-8"))
      .withRollingPolicy(DefaultRollingPolicy.builder().withRolloverInterval(60000L).build())
      .withBucketAssigner(new BucketAssigner[(String, Long, Double, Double), String] {
        override def getBucketId(element: (String, Long, Double, Double), context: BucketAssigner.Context): String = s"symbol=${element._1}"
        //override def getSerializer: SimpleVersionedStringSerializer[String] = SimpleVersionedStringSerializer.INSTANCE
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


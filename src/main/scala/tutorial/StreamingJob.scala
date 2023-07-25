package tutorial

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import Models.{CabFares, CabRide, Case_CabFares}
import Util.TweetParser
import Sources.CabEventSource
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.{MapFunction, RichFlatMapFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.{TumblingEventTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.twitter.TwitterSource
import org.apache.flink.util.Collector

import java.util.Properties
import scala.util.{Failure, Success}

/**
 * Skeleton for a Flink Streaming Job.
 *
 * For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
object StreamingJob {

  def main(args: Array[String]) {
    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //socketStreamingExample(env)
    //TwitterSourceExample(env)


    //CabRieExample(env);

    //    env
    //      .addSource(new CabEventSource())







    env.enableCheckpointing(10000)
    var txt = env.readTextFile("./src/main/resources/trip_fare_1k.csv")

    var cabFares_collections = txt.map(new MapFunction[String, CabFares] {
      override def map(row: String): CabFares = CabFares.fromString(row)

    })

    //    Find the total tips for a driver over last 1 day - keyby, window, apply aggregation






    var case_CabFares_stream: DataStream[Case_CabFares] = txt.map(new MapFunction[String, Case_CabFares] {
      override def map(row: String): Case_CabFares = {
        val token = row.split(",")

        // Using Try block to convert token(8) to Double and handle the result
        val token8AsDouble: Double = scala.util.Try(token(8).toDouble) match {
          case Success(value) => value // Conversion was successful
          case Failure(_) => 0.0 // Default value to use in case of conversion failure
        }

        return Case_CabFares(token(0), token(1), token(2), token(3), token(4), token(5), token(6), token(7), token8AsDouble, token(9), token(10))
      }
    })

    val watermarkStrategy = WatermarkStrategy
      .forMonotonousTimestamps[Case_CabFares]()
      .withTimestampAssigner(new SerializableTimestampAssigner[Case_CabFares] {
        override def extractTimestamp(t: Case_CabFares, l: Long): Long = {

          // Define the date format you are using in the input strings
          val formatter = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

          // Parse the date strings into LocalDateTime objects
          val dateTime = java.time.LocalDateTime.parse(t.pickup_datetime, formatter)

          // Convert LocalDateTime objects to long timestamps (milliseconds since epoch)
          val timestamp = dateTime.toInstant(java.time.ZoneOffset.UTC).toEpochMilli

          timestamp
        }
      })


    val withTimestampsAndWatermarks: DataStream[Case_CabFares] = case_CabFares_stream
      .assignTimestampsAndWatermarks(watermarkStrategy)


    var driver_hr = withTimestampsAndWatermarks
      .keyBy(_.hack_license)
      //.keyBy("hack_license")
      .window(TumblingEventTimeWindows.of(Time.minutes(10)))
      .process(new ProcessWindowFunction[Case_CabFares, String, String, TimeWindow] {
        override def process(key: String, context: Context, elements: Iterable[Case_CabFares], out: Collector[String]): Unit = {
          var cnt = 0;
          for (row <- elements) {
            cnt += 1;
          }
          out.collect("count is " + cnt + "for key" + key)
        }
      })

    //      .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
    //      .process(new ProcessWindowFunction[Case_CabFares, String, String,TimeWindow] {
    //        override def process(key: String, context: Context, elements: Iterable[Case_CabFares], out: Collector[String]): Unit = ???
    //      })
    //      //      .process(new ProcessWindowFunction[Case_CabFares,String,S] {})
    //      .process(new ProcessWindowFunction[Case_CabFares,String,String,TimeWindow]
    //      {
    //        //        override def process(key: String, context: Context, elements: Iterable[Case_CabFares], out: Collector[String]): Unit = {
    //        //        for ( x <- elements )
    //        //        {
    //        //          print(x)
    //        //
    //        //        }
    //        //
    //        //      }
    //
    //        override def process(key: String, context: ProcessWindowFunction[Case_CabFares, String, String, TimeWindow]#Context, iterable: lang.Iterable[Case_CabFares], collector: Collector[String]): Unit = ???
    //      })

    driver_hr.print();
    // execute program
    env.execute("Flink Streaming Scala API Skeleton")
  }

  def CabRieExample(environment: StreamExecutionEnvironment): Unit = {
    //    val taxiDS = environment.socketTextStream("localhost", 9999)
    //
    //    taxiDS
    //      .flatMap{str => str.split(" ")}
    //      .print();

    environment
      .addSource(new CabEventSource())
      .filter({ cr => !(cr.dropLocation.isEmpty && cr.pickLocation.isEmpty) })
      .map(new MapFunction[CabRide, String] {
        override def map(t: CabRide): String = t.toString
      })
      .flatMap(new RichFlatMapFunction[String, String] {

        var sum: ValueState[Int] = _

        override def open(parameters: Configuration): Unit = {
          sum = getRuntimeContext.getState(
            new ValueStateDescriptor[Int]("nameOfState", createTypeInformation[Int]))
        }

        override def flatMap(in: String, collector: Collector[String]): Unit = {
          var values = sum.value();
          collector.collect(in)
          sum.update(Integer.parseInt(in))
        }


      })
      .print();


    //    Print the data as is - Done
    //    Map each row to a TaxiRide object (create an object)
    //    Filter map each row to a TaxiRide object where only pick and drop locations are available
    //    Filter the rows where the start or end location is missing
  }

  private def TwitterSourceExample(env: StreamExecutionEnvironment) = {
    //please set the keys for your twitter API
    val props = new Properties()
    props.setProperty(TwitterSource.CONSUMER_KEY, "")
    props.setProperty(TwitterSource.CONSUMER_SECRET, "")
    props.setProperty(TwitterSource.TOKEN, "")
    props.setProperty(TwitterSource.TOKEN_SECRET, "")


    //Todo: parse the tweet, then filter it, then apply some other transformations and share the code
    //https://github.com/haseeb1431/flink-examples/blob/50e14ea4d2fc6483b0a5e59c4cbd52e83a231eb9/src/main/java/myflink/TwitterStreaming.java#L55
    //Assignment for the twitter
    //Filter, flatmap, map, keyby, coflatmap, richflatmap

    val streamSource: DataStream[String] = env.addSource(new TwitterSource(props))

    val tweets = streamSource
      .flatMap(new TweetParser)
      .map { (t) => (t.source, 1) }
      .keyBy(_._1)
      .sum(1)

    tweets.print()
  }

  private def socketStreamingExample(env: StreamExecutionEnvironment) = {
    val text = env.socketTextStream("localhost", 9999)
    val counts = text.map { (m: String) => (m.split(" ")(0), 1) }
      .keyBy(0)
      .sum(1)

    counts.print();
  }
}

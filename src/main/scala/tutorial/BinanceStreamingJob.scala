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

import java.io.FileInputStream
import java.util.Properties
import org.apache.flink.streaming.api.scala._
//import org.apache.flink.streaming.connectors.http._
//import org.apache.flink.streaming.connectors.http.utils.{AbstractHttpResponder, HttpResponderUtils}
import play.api.libs.json._


object BinanceStreamingJob {

  def main(args: Array[String]): Unit = {

    val api_key = "0MjBLvkhatZzlNla0DvUbuajvSyiLSJkOiMj1HF91ENtPe2sXp8e98a1UZCuwRbE"
    val api_secret = "a0Rnxdv5HiWzjOHaGJNxc48UaPmJPX0I9zn7hxE2bDbMMznMZfZtYuR7BNUiCFUy"

    // Read config file
    val configPath = "config.properties"
    val config = new Properties()

    // Read configuration properties
    config.load(getClass.getResourceAsStream(configPath))
    val outputFile = config.getProperty("outputFile")
    val symbolName = config.getProperty("symbolName")


    // Set up Flink environment
    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env

    // Define input stream from Binance websocket API
    val url = "wss://stream.binance.com:9443/ws/bnbusdt@ticker"
//    val httpStream = env.addSource(new HttpSource(url, classOf[String], new AbstractHttpResponder[String] {
//      override def onResponse(httpResponse: String): Unit = HttpResponderUtils.offerResponse(httpResponse, output)
//    }))
//
//    // Define output sink to CSV file
//    val outputFilePath = config.getProperty("outputFilePath")
//    val sink = StreamingFileSink
//      .forRowFormat(new Path(outputFilePath), new SimpleStringEncoder[String]("UTF-8"))
//      .withRollingPolicy(
//        DefaultRollingPolicy
//          .builder()
//          .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
//          .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
//          .withMaxPartSize(1024 * 1024 * 1024)
//          .build())
//      .build()
//
//    // Parse JSON and extract necessary data
//    val parsedStream = httpStream.map(jsonStr => {
//      val json = Json.parse(jsonStr)
//      val symbol = (json \ "s").as[String]
//      val bidPrice = (json \ "b").as[String]
//      val askPrice = (json \ "a").as[String]
//      (symbol, bidPrice, askPrice)
//    })

    // Write to output sink
//    parsedStream.map(tuple => tuple.productIterator.mkString(","))
//      .addSink(sink)

    // execute program
    env.execute("BinanceWebSocket")
  }
}

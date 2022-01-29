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

import Util.TweetParser
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.twitter.TwitterSource

import java.util.{Properties}

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

    TwitterSourceExample(env)

    /*
     * Here, you can start creating your execution plan for Flink.
     *
     * Start with getting some data from the environment, like
     *  env.readTextFile(textPath);
     *
     * then, transform the resulting DataStream[String] using operations
     * like
     *   .filter()
     *   .flatMap()
     *   .join()
     *   .group()
     *
     * and many more.
     * Have a look at the programming guide:
     *
     * https://flink.apache.org/docs/latest/apis/streaming/index.html
     *
     */

    // execute program
    env.execute("Flink Streaming Scala API Skeleton")
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

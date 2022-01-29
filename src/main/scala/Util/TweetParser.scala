package Util

import Models.Tweet
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.flink.util.Collector

import java.util.StringTokenizer
import scala.collection.mutable.ListBuffer


class TweetParser extends FlatMapFunction[String, Tweet] {

  lazy val jsonParser = new ObjectMapper()

  override def flatMap(s: String, collector: Collector[Tweet]): Unit = {

    // deserialize JSON from twitter source
    val jsonNode = jsonParser.readValue(s, classOf[JsonNode])
    val isEnglish = jsonNode.has("user") &&
      jsonNode.get("user").has("lang") &&
      jsonNode.get("user").get("lang").asText == "en"
    val hasText = jsonNode.has("text")

    (isEnglish, hasText, jsonNode) match {

      case (true, true, node) => {

        val tokens = new ListBuffer[Tweet]()
        val tokenizer = new StringTokenizer(node.get("text").asText())
        val userNode = node.get("user")
        var source = ""
        if (node.has("source")) {
          var source = node.get("source").asText.toLowerCase
          if (source.contains("android")) source = "Android"
          else if (source.contains("iphone")) source = "iphone"
          else if (source.contains("web")) source = "web"
          else source = "unknow"
        }

        val tweet = new Tweet(
          node.get("text").asText(),
          userNode.get("name").asText(),
          s,
          userNode.get("lang").asText(),
          source
        )

        collector.collect(tweet)

      }
      case _ =>
    }
  }

}
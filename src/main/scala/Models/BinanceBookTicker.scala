package Models

import play.api.libs.json.Json

case class BinanceBookTicker(
                              symbol: String,
                              bidPrice: Double,
                              bidQuantity: Double,
                              askPrice: Double,
                              askQuantity: Double,
                              updateId: Long,
                              eventTime: Long,
                              transactionTime: Long,
                              payload: String,
                              key: Int
                            )

object BinanceBookTicker {

  def fromString(jsonString: String): BinanceBookTicker = {

    val json = Json.parse(jsonString)

    val eventType = (json \ "e").as[String]
    val updateId = (json \ "u").as[Long]
    val eventTime = (json \ "E").as[Long]
    val transactionTime = (json \ "T").as[Long]
    val symbol = (json \ "s").as[String]
    val bidPrice = (json \ "b").as[String]
    val bidQuantity = (json \ "B").as[String]
    val askPrice = (json \ "a").as[String]
    val askQuantity = (json \ "A").as[String]


    BinanceBookTicker(symbol, bidPrice.toDouble, bidQuantity.toDouble,
      askPrice.toDouble, askQuantity.toDouble,
      updateId, eventTime, transactionTime, jsonString, 1)
  }

}

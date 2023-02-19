package Sources

import Models.BinanceBookTicker
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.java_websocket.client.WebSocketClient
import org.java_websocket.handshake.ServerHandshake
import play.api.libs.json.Json

class BinanceWebSocketSource(apiKey: String, secretKey: String, symbol: String) extends SourceFunction[BinanceBookTicker] {

  @volatile private var running = true

  override def run(ctx: SourceContext[BinanceBookTicker]): Unit = {

    //val url = s"wss://stream.binance.com:443/ws/${symbol.toLowerCase}@bookTicker"
    //val url = s"wss://ws-api.binance.com:443/ws-api/v3/${symbol.toLowerCase}@bookTicker"
    var url = s"wss://fstream.binance.com/ws/${symbol.toLowerCase}@bookTicker" //?listenKey=${apiKey}"

    val ws = new WebSocketClient(new java.net.URI(url)) {
      override def onOpen(handshakedata: ServerHandshake): Unit = {

        //https://github.com/binance-exchange/binance-java-api/blob/master/src/main/java/com/binance/api/client/security/HmacSHA256Signer.java
        //        val timestamp = System.currentTimeMillis()
        //        val signature = BinanceSignatureUtil.generateSignature(s"symbol=${symbol.toUpperCase}&timestamp=$timestamp", secretKey)
        //        val authMsg = s"""{"method":"SUBSCRIBE","params":["${symbol.toLowerCase}@bookTicker"],"id":1}"""
        //        send(authMsg)
      }

      override def onMessage(message: String): Unit = {

        val json = Json.parse(message)

        val eventType = (json \ "e").as[String]
        val updateId = (json \ "u").as[Long]
        val eventTime = (json \ "E").as[Long]
        val transactionTime = (json \ "T").as[Long]
        val symbol = (json \ "s").as[String]
        val bidPrice = (json \ "b").as[String]
        val bidQuantity = (json \ "B").as[String]
        val askPrice = (json \ "a").as[String]
        val askQuantity = (json \ "A").as[String]

        //        val json = io.circe.parser.parse(message).getOrElse(io.circe.Json.Null)
        //        val bidPrice = json.hcursor.get[Double]("b").getOrElse(0.0)

        //        val json = parse(message).getOrElse(Json.Null)
        //        val bidPrice = json.hcursor.downField("b").as[Double].getOrElse(0.0)
        val bookTicker = BinanceBookTicker(symbol, bidPrice.toDouble, bidQuantity.toDouble,
          askPrice.toDouble, askQuantity.toDouble,
          updateId, eventTime, transactionTime, message, 1)
        ctx.collect(bookTicker)
      }

      override def onClose(code: Int, reason: String, remote: Boolean): Unit = {
        println(s"WebSocket closed with code $code: $reason")
        running = false
      }

      override def onError(ex: Exception): Unit = {
        ex.printStackTrace()
        running = false
      }
    }

    ws.connect()
    while (running) {
      // keep running until the WebSocket connection is closed
    }
    ws.close()
  }

  override def cancel(): Unit = {
    running = false
  }
}


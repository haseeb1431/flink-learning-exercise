package Sources

import Models.BinanceBookTicker
import org.apache.flink.api.common.io.FileInputFormat
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.scala._
import play.api.libs.json._


class JsonFileSource(filePath: String) extends SourceFunction[BinanceBookTicker] {

  private var running: Boolean = true

  override def run(ctx: SourceFunction.SourceContext[BinanceBookTicker]): Unit = {

//
//
//
//
//    val fileInputFormat = new FileInputFormat[BinanceBookTicker](new Path(filePath)) {
//
//       def readRecord(reuse: BinanceBookTicker, bytes: Array[Byte], offset: Int, numBytes: Int): BinanceBookTicker = {
//        val jsonString = new String(bytes.slice(offset, offset + numBytes))
//        val json = Json.parse(jsonString)
//        BinanceBookTicker(
//          (json \ "s").as[String],
//        (json \ "b").as[String].toDouble,
//        (json \ "B").as[String].toDouble,
//        (json \ "a").as[String].toDouble,
//        (json \ "A").as[String].toDouble,
//        (json \ "u").as[Long],
//        (json \ "E").as[Long],
//        (json \ "T").as[Long],
//          jsonString
//        )
//      }
//    }
//
//    fileInputFormat.setDelimiter("\n")
//    val stream: DataStream[BinanceBookTicker] = ctx
//      .env
//      .createInput(fileInputFormat, fileInputFormat.getProducedType())
//      .map(record => record)

//    while (running) {
////      stream.addSink(new SinkFunction[BinanceBookTicker]() {
////        override def invoke(value: BinanceBookTicker): Unit = {
////          ctx.collect(value)
////        }
//      })
//      ctx.markAsTemporarilyIdle()
//      Thread.sleep(1000) // Wait for 1 second before trying to read again
//    }
  }

  override def cancel(): Unit = {
    running = false
  }
}

package Sources

import Models.CabRide
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import java.time.Instant
import java.util
import java.util.{ArrayList, List, Random}

object CabEventSource {
  private val carTypeList: List[String] = new ArrayList[String]
  private val driverNameList: List[String] = new ArrayList[String]
  private val pickDropList: List[String] = new ArrayList[String]

  try carTypeList.add("SUV")
  carTypeList.add("LUV")
  carTypeList.add("Mini")
  carTypeList.add("Sedan")
  driverNameList.add("Lee")
  driverNameList.add("Hannah")
  driverNameList.add("Haseeb")
  driverNameList.add("Riikak")
  driverNameList.add("Nina")
  pickDropList.add("sector 1")
  pickDropList.add("sector 2")
  pickDropList.add("sector 3")
  pickDropList.add("sector 4")
  pickDropList.add("sector 5")
  pickDropList.add("sector 6")
  pickDropList.add("sector 7")
  pickDropList.add("sector 8")

}

class CabEventSource extends RichParallelSourceFunction[CabRide] {

  private var isRunning: Boolean = true

  @throws[Exception]
  override def run(sourceContext: SourceFunction.SourceContext[CabRide]): Unit = {

    val rand: Random = new Random
    while ( {
      isRunning
    }) {
      val currTime: Long = Instant.now.toEpochMilli

      val cr: CabRide = new CabRide

      cr.eventTime  = currTime
      cr.id = "1"
      cr.ongoingTrip = "yes"
      cr.numberPlate = String.valueOf(rand.nextGaussian)
      cr.carType = CabEventSource.carTypeList.get(rand.nextInt(CabEventSource.carTypeList.size))
      cr.driverName = CabEventSource.driverNameList.get(rand.nextInt(CabEventSource.driverNameList.size))
      cr.pickLocation = CabEventSource.pickDropList.get(rand.nextInt(CabEventSource.pickDropList.size))
      cr.dropLocation = CabEventSource.pickDropList.get(rand.nextInt(CabEventSource.pickDropList.size))
      cr.passengerCount = rand.nextInt(10)

      for (i <- 0 until 5) {
        sourceContext.collect(cr)
      }

      Thread.sleep(Time.seconds(5).toMilliseconds)
    }
  }

  override def cancel(): Unit = {
    isRunning = false
  }
}

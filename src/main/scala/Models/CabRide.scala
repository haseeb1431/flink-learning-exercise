package Models

object CabRide {

  def fromString(s: String): CabRide = {

    val ride = new CabRide
    val tokens = s.split(",")

    ride.id = tokens(0)
    ride.numberPlate = tokens(1)
    ride.carType = tokens(2)
    ride.driverName = tokens(3)
    ride.ongoingTrip = tokens(4)
    ride.pickLocation = tokens(5)
    ride.dropLocation = if (tokens(6) == "'null'") null
    else tokens(6)
    ride.passengerCount = if (tokens(7) == "'null'") 0
    else tokens(7).toInt

    ride

  }
}

class CabRide() {
  //#cab id, cab number plate, cab type, cab driver name, ongoing trip/not, pickup location, destination,passenger count
  var id: String = _
  var numberPlate: String = _
  var carType: String = _
  var driverName: String = _
  var ongoingTrip: String = _
  var pickLocation: String = _
  var dropLocation: String = _
  var passengerCount: Integer = _
  var eventTime: Long = Long.MinValue

  override def hashCode: Int = this.driverName.hashCode + this.dropLocation.hashCode * 31

  override def toString: String = this.id + " " + this.numberPlate + " " + this.driverName + " " +
    this.pickLocation + " " + this.dropLocation + " "
}


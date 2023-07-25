package Models

object CabFares {
  def fromString(s: String) : CabFares = {

    val fare = new CabFares
    val token = s.split(",")
    fare.medallion = token(0)
    fare.hack_license  = token(1)
    fare.vendor_id  = token(2)
    fare.pickup_datetime  = token(3)
    fare.payment_type  = token(4)
    fare.fare_amount  = token(5)
    fare.surcharge  = token(6)
    fare.mta_tax  = token(7)
    fare.tip_amount  = token(8)
    fare.tolls_amount  = token(9)
    fare.total_amount  = token(10)
    //    ride.pass_cnt = if (tokens(7)!= null && tokens(7)!= "'null'" ) {tokens(7).toInt } else 0
    return fare
  }

}

class CabFares() {

  var medallion: String = _
  var hack_license : String = _
  var vendor_id : String = _
  var pickup_datetime : String = _
  var payment_type : String = _
  var fare_amount : String = _
  var surcharge : String = _
  var mta_tax : String = _
  var tip_amount : String = _
  var tolls_amount : String = _
  var total_amount :  String = _

  override def hashCode: Int = this.hack_license.hashCode + this.vendor_id.hashCode * 31

  override def toString: String = (medallion + " " + hack_license + " " + vendor_id + " " + pickup_datetime + " " + payment_type + " "
    + fare_amount + " "
    + surcharge + " "
    + mta_tax + " "
    + tip_amount + " "
    + tolls_amount + " "
    + total_amount
    )
}


case class Case_CabFares (
                           medallion: String ,
                           hack_license : String ,
                           vendor_id : String ,
                           pickup_datetime : String ,
                           payment_type : String ,
                           fare_amount : String ,
                           surcharge : String ,
                           mta_tax : String ,
                           tip_amount : Double ,
                           tolls_amount : String ,
                           total_amount :  String

                         )

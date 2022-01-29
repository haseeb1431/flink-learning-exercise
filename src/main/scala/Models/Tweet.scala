package Models


class Tweet(val textc: String, val userNamec: String, val rawTextc: String, val langc:String, val sourcec: String) {

  var text: String = textc
  val userName: String = userNamec
  val rawText: String = rawTextc
  val lang:String = langc
  val source: String = sourcec

}





package sparkml.rs.recall

case class ItemPref(userid: String, itemid: String, pref: Double)

case class ItemSimi(itemidI: String, itemidJ: String, similar: Double)

case class UserRecomm(userid: String, itemid: String, pref: Double)

case class ItemAssociation(itemidI:String, itemidJ:String, support:Double, confidence:Double, lift:Double, similar:Double)
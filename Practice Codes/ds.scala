import scala.collection.mutable._

object main extends App{
    val m=  Map("USA" -> "Washington D.C.","UK" -> "London", "India" -> "New Delhi")
    println(m("USA"))
    m("USA")="KLOP"
    println(m("USA"))
    for ((k, v)<- m) print( s"$k : $v \t")
    val t = (1, "hello", 20.3, true)
    println(t._2)

}
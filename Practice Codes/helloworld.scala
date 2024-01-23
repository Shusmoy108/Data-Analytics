// object HelloWorld{
//     def main(args:Array[String])=
//     {
//         println("Hello WOrld")
//     }
// }
import scala.collection.immutable._

def sumList(list:List[Int]):Int
{
    var s=0
    for (i<-list){
        s=s+i
    }
    
    println(s)
    s
}
    println("helo scala".sorted)
    val name = "Mark"; val age = 5
    print(f"Hello, $name! In six months, you'll be ${age + 0.5}%7.2f years old.%n")
    var x=for (i <- 1 to 10) yield i % 3
    print(x)
    sumList((1 to 10).toList)
}

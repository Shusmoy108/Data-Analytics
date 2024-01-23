import scala.math._

object main extends App{
def maximum(x:Double, y:Double):Double= if (x>y) x else y
def power (m:Int, n:Int)={
    var result:Int =1
    for (i<-1 to m)
        result*=n
    result
}
def raise(m:Int, n:Int):Int= if (n==0) 1 else m*raise(m,n-1) 
def sayName(name:String):Unit={
    println("Hello "+name)
}
val num=3.14
val f= ceil
def f2(f:(Double)=>Double, n:Double)= f(n)
var a=(x:Int)=>{
    x+100
}
def d2(f:(Double)=>Double, n:Double)={
    f(n)
}
println(d2((x:Double)=>0.25*x,12))
println(d2(0.25*_,12))
println(a(6))
println(f2(sqrt,9))
println(f(num))
println(maximum(3,5))
println(raise(2,3))
sayName("Shusmoy")
val names: Array[String]= new Array[String](3)
println(names)
val arr = Array(1,7,2,9)
for (i<-0 to arr.length-1)
{
    println(arr(i))
}
println(arr.sum)
}
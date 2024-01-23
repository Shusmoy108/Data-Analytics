class Counter{
    private var value=0
    def increment()={value+=1}
    def current()= value
}
class Person {
private var privateAge = 0 // Make private and rename
def age = privateAge
def age_= ( newAge : Int )= {
if (
newAge > privateAge ) privateAge = newAge ;
}
}
object main extends App{
val myCounter= new Counter
myCounter.increment()
println(myCounter.current())
val p= new Person()
p.age=89
println(p.age)
p.age=86
println(p.age)
}
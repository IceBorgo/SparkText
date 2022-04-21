package SparkGroupFavTeacher

import scala.collection.mutable

object TextTreeSret extends App{

   private val ints = mutable.TreeSet(("d",1),("a",5))

    println( ints.head._1)
    println(ints.head)
  ints.remove(ints.head)
  print(ints)



}

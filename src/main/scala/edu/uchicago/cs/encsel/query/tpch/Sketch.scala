package edu.uchicago.cs.encsel.query.tpch

import scala.io.Source

object Sketch extends App {

  var pkmax = 0
  var limax = 0
  var pkmin = Integer.MAX_VALUE
  var limin = Integer.MAX_VALUE

  Source.fromFile("/Users/harper/TPCH/lineitem.tbl").getLines().foreach(line=>{
    val data = line.split("\\|")
    val pk = data(1).toInt
    val li = data(3).toInt

    pkmax = Math.max(pkmax,pk)
    pkmin = Math.min(pkmin,pk)
    limax = Math.max(limax,li)
    limin = Math.min(limin,li)
  })

  println(pkmax)
  println(Math.log(pkmax)/Math.log(2))
  println(pkmin)
  println(limax)
  println(limin)
}

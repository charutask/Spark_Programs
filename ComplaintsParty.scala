package com.Analysis.test
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object ComplaintsParty {
  def main(Args:Array[String]){
    
    val conf = new SparkConf()
    .setAppName("sampleprogram")
          .setMaster("local")
    val sc = new SparkContext(conf)
    
    
    val filerdd = sc.textFile("C:/Users/Owner/Desktop/Scala_programs/partyComplaints.csv")
    val splitrdd = filerdd.flatMap(f => f.split(","))
    val pairrdd = splitrdd.map(w =>(w,1))
    val shufflerdd= pairrdd.reduceByKey((p,q)=>(p+q))
    
    //shufflerdd.collect().foreach(println)
    val sortrdd = shufflerdd.map(f=>(f._2,f._1))
    //sortrdd.collect().foreach(println)
    val sortnewrdd=sortrdd.sortBy(f=>f._1,false)
    //sortnewrdd.collect().foreach(println)
    val sort1rdd = sortnewrdd.first()
    println(sort1rdd)
  
  
  }
}
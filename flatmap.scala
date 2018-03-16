package com.Analysis.test

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object flatmap {
   def main(Args:Array[String]){
    
    val conf = new SparkConf()
    .setAppName("sampleprogram")
          .setMaster("local")
    val sc = new SparkContext(conf)
    
    val filerdd = sc.textFile("C:/Users/Owner/Desktop/Scala_programs/readfile.txt")
    val splitrdd = filerdd.flatMap(f => f.split(" "))
    println("Total no of words = " + splitrdd.count())
  splitrdd.collect().foreach(println)    
    
    }
}
package com.Analysis.test
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object filtereven {
  def main(Args:Array[String]){
      val conf = new SparkConf()
    .setAppName("sampleprogram")
          .setMaster("local")
    val sc = new SparkContext(conf)
    
   val intrdd = sc.parallelize(Array(1,2,3,4,5,6,7,8,9,10))
   val evennumberfilterrdd = intrdd.filter(i => (i%2 == 0))
   evennumberfilterrdd.collect().foreach(println)
}
}
package com.Analysis.test
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object countchar {
 def main(Args:Array[String]){
    
    val conf = new SparkConf()
    .setAppName("sampleprogram")
          .setMaster("local")
    val sc = new SparkContext(conf)
    
    //read the file
    val filerdd = sc.textFile("C:/Users/Owner/Desktop/Scala_programs/readfile.txt")
    
    //mapping and spilting the words
    val splitrdd = filerdd.flatMap( f => f.split(" "))
    val firstcharrdd = splitrdd.map(f => f.charAt(0).toString())
    //firstcharrdd.collect().foreach(println)
    
    val pairrdd = firstcharrdd.map(f => (f,1))
   // pairrdd.collect().foreach(println)
  
    // split by char 
    val splitrdd1 = filerdd.flatMap( f => f.split(""))
     //splitrdd1.collect().foreach(println)
    val charsrdd = splitrdd1.map(f => (f,1))
    val reduce_map_rdd = pairrdd.reduceByKey((p,q)=>p+q)
   reduce_map_rdd.collect().foreach(println)
    //val joinrdd = pairrdd.join(reduce_map_rdd)
   // joinrdd.collect().foreach(println)
 }
}
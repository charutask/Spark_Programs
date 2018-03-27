package com.Analysis.test
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


object movie_filter {
  
  def main(Args:Array[String]){ 
    val conf = new SparkConf()
    .setAppName("sampleprogram")
          .setMaster("local")
    val sc = new SparkContext(conf)
    
    //Program is written for reading the movie file and filtering the row directed by "Robert Wise" 
    val movierdd = sc.textFile("C:/Users/Owner/Desktop/Scala_programs/movie.csv")
    val line_movie_rdd= movierdd.filter(line =>line.contains("Robert"))
    
    //movierdd.collect().foreach(println)
    line_movie_rdd.collect().foreach(println)
    
}
}
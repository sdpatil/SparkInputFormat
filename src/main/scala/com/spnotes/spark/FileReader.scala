package com.spnotes.spark

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by sunilpatil on 4/18/16.
  */
object FileReader {

  def main(args:Array[String]): Unit ={
    if(args.length != 1){
      println("Please specify <filepath>")
      System.exit(-1)
    }
    val directoryPath = args(0)
    println(s"Reading data from $directoryPath")
    val sparkConf = new SparkConf().setAppName("FileReader").setMaster("local[*]")
    val sparkContext = new SparkContext(sparkConf);
    // Set custom delimiter for text input format
    sparkContext.hadoopConfiguration.set("textinputformat.record.delimiter",".")
    val sentences = sparkContext.textFile(directoryPath)
    println("Number of lines " + sentences.count())
    sentences.take(10).foreach(println)
  }

}

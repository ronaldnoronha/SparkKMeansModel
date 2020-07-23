package example.stream

import java.time.LocalDateTime

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer

import scala.io.Source
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.{KMeansModel, StreamingKMeansModel}
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingKMeansModelExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("StreamingKMeansModelExample")
    val ssc = new StreamingContext(conf,Seconds(1))

    val filename = "/home/ronald/random_centers.csv"
    val lines = Source.fromFile(filename).getLines.toArray.map(_.split(","))

    val centers:Array[linalg.Vector] = new Array[linalg.Vector](lines.length-1)
    for (i <- 1 to lines.length-1) {
      centers(i-1) = Vectors.dense(lines(i).map(_.toDouble))
    }
    val weights:Array[Double] = new Array[Double](centers.length)
    for (i<-0 to weights.length-1) {
      weights(i) = 1/centers.length
    }

    val model = new StreamingKMeansModel(centers,weights)

    val brokers = args(0)
    val groupId = args(1)
    val topics = args(2)

    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer])
    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))

    val inputLines = messages.map(_.value).map(_.split(","))

    val timestamp = inputLines.map(_(0)).map(_+" "+LocalDateTime.now().toString())
    val coords = inputLines.map(_(1).split(" ").map(_.toDouble)).map(x => Vectors.dense(x))
    val target = inputLines.map(_(2).toInt)
    coords.foreachRDD(rdd => {
      model.update(rdd, 1.0, "batches")
      println("Centers:")
      for (i <- model.clusterCenters) {
        println(i.toString())
      }
      println("Cluster Weights:")
      for (i <- model.clusterWeights) {
        println(i.toString())
      }
    })
//    println(model.toPMML())
    ssc.start()
    ssc.awaitTerminationOrTimeout(1200000)
    ssc.stop()
    val sc = new SparkContext(conf)
    model.save(sc, "/home/ronald/kmeansModel")

//    val sameModel = KMeansModel.load(sc,"/home/ronald/kmeansModel")
//    val modelCenters = sameModel.clusterCenters
//
//    println("Centers final:")
//    for (i <- modelCenters) {
//      println(i.toString())
//    }
//
//    val centerLines = Source.fromFile("/home/ronald/centers.csv").getLines.toArray.map(_.split(","))
//    val realCenters:Array[linalg.Vector] = new Array[linalg.Vector](centerLines.length-1)
//    for (i <- 1 to centerLines.length-1) {
//      realCenters(i-1) = Vectors.dense(centerLines(i).map(_.toDouble))
//    }
//    val totalCost = 0.0
//    val dist: Array[Array[Double]] = new Array[Array[Double]](realCenters.length)
//    for (i <- 0 to modelCenters.length -1) {
//      dist(i) = new Array[Double](realCenters.length)
//      for (j <- 0 to realCenters.length - 1) {
//        dist(i)(j) = scala.math.pow(Vectors.sqdist(modelCenters(i),realCenters(j)), 0.5)
////        println("Model Center: " + i.toString())
////        println("Real Center: " + realCenters(j).toString())
////        println("Distance: " + scala.math.pow(Vectors.sqdist(i, realCenters(j)), 0.5))
////        println("Minimum Distance: "+ minDist)
////        if (minDist > scala.math.pow(Vectors.sqdist(i, realCenters(j)), 0.5)) {
////          val minDist = scala.math.pow(Vectors.sqdist(i, realCenters(j)), 0.5)
////          val centerNum = j
////          println(centerNum)
////          println("Minimum Distance: "+ minDist)
////        }
//      }
//      println(dist(i))
//    }
//
////    println("Results:")
////    for (i <- realCenters) {
////      val minDist = Double.MaxValue
////
////      for (j <- 0 to sameModel.clusterCenters.length-1) {
////        if (minDist > scala.math.pow(Vectors.sqdist(i,sameModel.clusterCenters(j)),0.5)) {
////          val minDist = scala.math.pow(Vectors.sqdist(i,sameModel.clusterCenters(j)),0.5)
////          val minCenter = j
////        }
////      }
////      println(i.toString() + sameModel.clusterCenters() + minDist)
////      val totalCost = totalCost + minDist
////    }
////    println("Total Cost: ",totalCost)
  }
}


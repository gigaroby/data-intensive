import java.util.HashMap

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import kafka.serializer.{DefaultDecoder, StringDecoder}

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel

object KafkaWordCount {
  def main(args: Array[String]) {

    val kafkaConf = Map(
          "metadata.broker.list" -> "localhost:9092",
          "zookeeper.connect" -> "localhost:2181",
          "group.id" -> "kafka-spark-streaming",
          "zookeeper.connection.timeout.ms" -> "1000")

    val sparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("checkpoint")

    // if you want to try the receiver-less approach, comment the below line and uncomment the next one
    val messages = KafkaUtils.createStream[String, String, DefaultDecoder, StringDecoder](
      ssc,
      kafkaConf,
      Map("avg" -> 1),
      StorageLevel.MEMORY_ONLY
    )
    //val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](<FILL IN>)

    val values = messages.map(_._2)
    val pairs = values.map(x => {
      val splits = x.split(",")
      (splits(0),splits(1).toDouble)
    })

    def mappingFunc(key: String, value: Option[Double], state: State[(Int, Double)]): Option[(String, Double)] = {

      val stateValue = state.getOption.getOrElse((0,0.0))
      val sum = value.getOrElse(0.0) + stateValue._2
      val counter = stateValue._1 + 1
      val avg = sum/counter

      state.update((counter, sum))
      return Some((key, avg))
    }

    val stateDstream = pairs.mapWithState(StateSpec.function(mappingFunc _))

    stateDstream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}

package Exercises.part4

import org.apache.spark.sql.SparkSession
import common._
import org.apache.spark.sql.functions._

object KafkaStream {

  def readCars(spark: SparkSession) = {
    spark.readStream.schema(carsSchema).json("src/main/resources/data/cars")
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("Writing cars to kafka")
      .getOrCreate()

    val cars = readCars(spark)

    cars
      .select(
        col("Name").as("key"),
        to_json(struct(col("*"))).as("value")
      ).writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "rockthejvm")
      .option("checkpointLocation", "checkpoints")
      .start()
      .awaitTermination()
  }

}

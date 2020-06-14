package Exercises.part2

import org.apache.spark.sql.{Dataset, SparkSession}
import common._
import org.apache.spark.sql.functions._

object ExDatasets {

  def loadCarStream(spark: SparkSession): Dataset[Car] = {
    import spark.implicits._
    spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .select(from_json(col("value"), carsSchema).as("car"))
      .selectExpr("car.*")
      .as[Car]
  }

  def procPowerfullCars(ds: Dataset[Car]): Unit = {
    ds
      .filter(_.Horsepower.getOrElse(0L) > 140L)
      .select(count("*"))
      .writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def averageHP(ds: Dataset[Car]): Unit = {
    ds.agg(
      avg("HorsePower")
    ).writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def carsByOrigin(ds: Dataset[Car]): Unit = {
    //ds.groupBy(_.Horsepower)
    ds.groupBy("Origin")
      .count()
      .writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Exercise Datasets")
      .config("spark.master", "local[*]")
      .getOrCreate()

    val ds: Dataset[Car] = loadCarStream(spark)
    averageHP(ds)
  }
}

package Exercises.part5

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}

object WindowExercises {

  val onlinePurchaseSchema = StructType(Array(
    StructField("id", StringType),
    StructField("time", TimestampType),
    StructField("item", StringType),
    StructField("quantity", IntegerType)
  ))

  def readPurchaseFromSocket(spark: SparkSession): DataFrame = {
    spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .select(from_json(col("value"), onlinePurchaseSchema).as("purchase"))
      .selectExpr("purchase.*")
  }

  def readPurchaseFromFile(spark: SparkSession): DataFrame = {
    spark.readStream
      .schema(onlinePurchaseSchema)
      .json("src/main/resources/data/purchases")

  }

  def topSoldByDay(sparkSession: SparkSession, stream: DataFrame) = {
    stream
      .groupBy(window(col("time"), "1 day") as "day", col("item"))
      .agg(
        sum(col("quantity")) as "Amount"
      ).select(
        col("day").getField("start") as "Start",
        col("day").getField("end") as "End",
        col("item"),
        col("Amount")
      )
      .orderBy(col("day"), col("Amount").desc_nulls_last)
  }

  def topSoldByDayWithSlidingWindow(sparkSession: SparkSession, stream: DataFrame) = {
    stream
      .groupBy(window(col("time"), "1 day", "1 hour") as "hourlyDay", col("item"))
      .agg(
        sum(col("quantity")) as "Amount"
      ).select(
        col("hourlyDay").getField("start") as "Start",
        col("hourlyDay").getField("end") as "End",
        col("item"),
        col("Amount")
      )
      .orderBy(col("Start"), col("Amount").desc_nulls_last)
  }


  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Window Exercise")
      .master("local[*]")
      .getOrCreate()

    val stream = readPurchaseFromFile(spark)

    val res = topSoldByDay(spark, stream)

    res.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

}

package org.apache.spark.examples

import org.apache.spark.sql.SparkSession

object FirstTest {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .master("local[*]")
      .getOrCreate()

    print(spark)

  }

}

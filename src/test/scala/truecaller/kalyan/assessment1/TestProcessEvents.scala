package truecaller.kalyan.assessment1

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, collect_list, desc, lit, map, row_number}
import org.scalatest.FunSuite

import java.io.File

class TestProcessEvents extends FunSuite{

  test("Not a test case, JUST space for local execution") {

    /* Create Spark Session */
    val warehouseLocation = new File("/tmp/spark-warehouse").getAbsolutePath
    val spark = SparkSession
      .builder()
      .appName("Event Data Processing")
      .config("spark.master", "local")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    ProcessEvents.setup( spark )

    val events = spark.read.table("default.events_data")

    val filteringWindow = Window.partitionBy(col("id"), col("name")).orderBy(desc("timestamp"))
    val latest_events = events.withColumn("latest_event", row_number().over(filteringWindow)
                                          ).orderBy("id", "timestamp"
                                          ).filter("latest_event = 1")

    //Create a List of columns ready to be mapped
    val settingsMap = List("name", "value").flatMap(colName => List(lit(colName), col(colName)))
    val result = latest_events.withColumn("settings", map(settingsMap: _ *)
                            ).drop("name", "value", "timestamp", "latest_event")

    val shufflePartitionsSize = 3
    result.withColumn("hashed_id", col("id").mod(shufflePartitionsSize)
    ).repartition(shufflePartitionsSize, col("id")
    ).sortWithinPartitions("id"
    ).write.format("parquet"
    ).partitionBy("hashed_id"
    ).mode("overwrite"
    ).saveAsTable("default.latest_settings")

    /*result.write.format("parquet").mode("overwrite").saveAsTable("default.result")
    result.printSchema()*/
    spark.sql("SELECT * FROM default.latest_settings").show(false)

    spark.stop()

  }

}

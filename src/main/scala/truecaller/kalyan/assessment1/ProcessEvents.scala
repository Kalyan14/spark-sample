package truecaller.kalyan.assessment1

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

import java.io.File

object ProcessEvents {

  def main(args: Array[String]): Unit = {

    /*
    * THOUGHTS:
    *
    *   If given option, I would propose changes to the way the event table is designed.
    *   Suggestion on the way the event table could be designed:
    *     Store the event in hash partitioned table based on the ID
    *     Since the problem is SEMI-DETERMINISTIC, it would have been less shuffle while transforming.
    *
    *   For the assessment sake, I am not assuming any changes in the format provided.
    *
    * */

    val warehouseLocation = new File("/tmp/spark-warehouse").getAbsolutePath

    /* Create Spark Session */
    val spark = SparkSession
      .builder()
      .appName("Event Data Processing")
      .config("spark.master", "local")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    /* SETUP sample events data for assignment */
    setup( spark )

    /* Solution Overview:
    *   STEP-1: Divide the problem into 2 steps
    *           Filtering only latest events per ID and event name.
    *
    *   STEP-2: Aggregate all the event name and values to Map type per ID
    *
    *   STEP-3: Goal is to create partitioned table but we SHOULD NOT create partition on the high-cardinality column
    *           For now, solve with hash partition (Use Modulus hash function here for simplicity)
    * */

    /* STEP-1: Read data and filter out old data */
    val events          = spark.read.table("default.events_data")
    val filteringWindow = Window.partitionBy( col("id"), col("name") ).orderBy(desc("timestamp") )
    val latest_events   = events.withColumn("latest_event", row_number().over(filteringWindow)
                                            ).orderBy("id", "timestamp"
                                            ).filter("latest_event = 1"
                                            ).drop("timestamp", "latest_event")

    /* STEP-2: */
    val settingsMap = List("name", "value").flatMap(colName => List(lit(colName), col(colName)))
    val mappedSettings = latest_events.withColumn("settings", map(settingsMap: _ *)
                                                  ).drop("name", "value")

    /* STEP-3 */
    val shufflePartitionsSize = 5
    mappedSettings.withColumn("hashed_id", col("id").mod(shufflePartitionsSize)
                              ).repartition(shufflePartitionsSize, col("id")
                              ).sortWithinPartitions("id"
                              ).write.format("parquet"
                              ).partitionBy("hashed_id"
                              ).mode("overwrite"
                              ).saveAsTable("default.latest_settings")

    /* ADVANTAGES OF THIS PARTITIONING APPROACH
    *
    *   Let's examine a couple of files written by this approach
    *

    >>> pd.read_parquet("./hashed_id=0/part-00001-50e8c9b3-54f4-4a51-a01e-97e3aaa00ac6.c000.snappy.parquet")
        id                             settings
    0   18          [(name, S), (value, FALSE)]
    1   18           [(name, F), (value, TRUE)]
    2   33  [(name, D), (value, UNDERTERMINED)]
    3   33           [(name, G), (value, TRUE)]
    4   33           [(name, E), (value, TRUE)]
    5   33          [(name, C), (value, FALSE)]
    6   48           [(name, C), (value, TRUE)]
    7   54  [(name, D), (value, UNDERTERMINED)]
    8   54  [(name, L), (value, UNDERTERMINED)]
    9   66           [(name, D), (value, TRUE)]
    10  87  [(name, D), (value, UNDERTERMINED)]
    11  99           [(name, A), (value, TRUE)]

    >>> pd.read_parquet("./hashed_id=0/part-00000-50e8c9b3-54f4-4a51-a01e-97e3aaa00ac6.c000.snappy.parquet")
       id                             settings
    0  24          [(name, D), (value, FALSE)]
    1  30           [(name, K), (value, TRUE)]
    2  30          [(name, S), (value, FALSE)]
    3  30           [(name, H), (value, TRUE)]
    4  30  [(name, G), (value, UNDERTERMINED)]
    5  30           [(name, A), (value, TRUE)]
    6  51           [(name, G), (value, TRUE)]
    7  72          [(name, Y), (value, FALSE)]
    8  78          [(name, D), (value, FALSE)]
    9  78          [(name, J), (value, FALSE)]
    *
    * OBSERVATION: From both the files, `id` values are sorted in ascending order
    *
    * ADVANTAGE: Speed on Reads (when predicat-pushdown setting is enabled)
    *            Spark will just read metadata and extracts data from the required stripes of each file.
    *            This method also helps to control the number of partitions instead of millions.
    * */

    spark.stop()
  }

  def setup( spark: SparkSession ): Unit = {

    /* Read data from the sample events */
    val sampleEventFiles  = "/home/saikalyan/IdeaProjects/spark-sample/src/main/resources/source-events"
    val eventsSchema    = new StructType()
                          .add("id",LongType,false)
                          .add("name",StringType,false)
                          .add("value", StringType, false)
                          .add("timestamp", LongType, false)
    val sampleEventsDf  = spark.read.option("header", "true").option("inferSchema", "true").schema(eventsSchema).csv(sampleEventFiles)

    sampleEventsDf.write.format("parquet").mode("overwrite").saveAsTable("default.events_data")

  }
}

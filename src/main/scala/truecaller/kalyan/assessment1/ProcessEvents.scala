package truecaller.kalyan.assessment1

import org.apache.spark.sql.SparkSession

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

  }

  def helloWorld(): Unit = {

    val warehouseLocation = new File("/home/saikalyan/IdeaProjects/spark-sample/src/main/resources/spark-warehouse").getAbsolutePath

    val spark = SparkSession
      .builder()
      .appName("Event Data Processing")
      .config("spark.master", "local")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()


    print("EXECUTED HERE =========")
    spark.sql("SHOW DATABASES ").show(false)

    spark.stop()
  }
}

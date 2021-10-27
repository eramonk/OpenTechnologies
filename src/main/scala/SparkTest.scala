import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._


object SparkTest extends App {

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("SparkTest")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  val source: DataFrame = spark.sqlContext.read.parquet("/home/ra/test/scala_test_data/data/dev_ind/dev*/*")
  val sourceWithUnixTime = source.filter(row => row(6).toString.contains("PV"))
    .withColumn("time", from_unixtime(element_at(split(col("_time_range"), "-"), 2), "HH:mm:ss"))
    .withColumn("unix_time", element_at(split(col("_time_range"), "-"), 2))

  val percentile = sourceWithUnixTime.groupBy(col("time"))
    .agg(
      callUDF("percentile_approx", col("value"), lit(0.9)).as("value"),
      first("unix_time").as("unix_time_p")
    )

  val w1h = Window.partitionBy(col("datatype")).orderBy(col("unix_time").cast("long")).rangeBetween(-(60*50), Window.currentRow)
  val w12h = Window.partitionBy(col("datatype")).orderBy(col("unix_time").cast("long")).rangeBetween(-(60*60*12+600), Window.currentRow)
  val w24h = Window.partitionBy(col("datatype")).orderBy(col("unix_time").cast("long")).rangeBetween(-(60*30*24+600), Window.currentRow)
  val w120h = Window.partitionBy(col("datatype")).orderBy(col("unix_time").cast("long")).rangeBetween(-(60*30*120+600), Window.currentRow)

  val metrics = sourceWithUnixTime
    .withColumn("val_std_1h", stddev_pop(col("value")).over(w1h)).orderBy(asc("unix_time"))
    .withColumn("val_std_12h", stddev_pop(col("value")).over(w12h)).orderBy(asc("unix_time"))
    .withColumn("val_std_24h", stddev_pop(col("value")).over(w24h)).orderBy(asc("unix_time"))
    .withColumn("val_std_120h", stddev_pop(col("value")).over(w120h)).orderBy(asc("unix_time"))
    .withColumn("val_mean_1h", mean(col("value")).over(w1h)).orderBy(asc("unix_time"))
    .withColumn("val_mean_12h", mean(col("value")).over(w12h)).orderBy(asc("unix_time"))
    .withColumn("val_mean_24h", mean(col("value")).over(w24h)).orderBy(asc("unix_time"))
    .withColumn("val_mean_120h", mean(col("value")).over(w120h)).orderBy(asc("unix_time"))
    .withColumn("val_z_score_1h", (col("value") - col("val_mean_1h")) / col("val_std_1h")).orderBy(asc("unix_time"))
    .withColumn("val_z_score_12h", (col("value") - col("val_mean_12h")) / col("val_std_12h")).orderBy(asc("unix_time"))
    .withColumn("val_z_score_24h", (col("value") - col("val_mean_24h")) / col("val_std_24h")).orderBy(asc("unix_time"))
    .withColumn("val_z_score_120h", (col("value") - col("val_mean_120h")) / col("val_std_120h")).orderBy(asc("unix_time"))
    .withColumn("val_ratio_1h", (col("value") - first(col("value")).over(w1h)) / first(col("value")).over(w1h)).orderBy(asc("unix_time"))
    .withColumn("val_ratio_12h", (col("value") - first(col("value")).over(w12h)) / first(col("value")).over(w12h)).orderBy(asc("unix_time"))
    .withColumn("val_ratio_24h", (col("value") - first(col("value")).over(w24h)) / first(col("value")).over(w24h)).orderBy(asc("unix_time"))
    .withColumn("val_ratio_120h", (col("value") - first(col("value")).over(w120h)) / first(col("value")).over(w120h)).orderBy(asc("unix_time"))
    .groupBy("unix_time").agg(
    first("val_std_1h").as("val_std_1h"),
    first("val_std_12h").as("val_std_12h"),
    first("val_std_24h").as("val_std_24h"),
    first("val_std_120h").as("val_std_1h"),
    first("val_mean_1h").as("val_mean_1h"),
    first("val_mean_12h").as("val_mean_12h"),
    first("val_mean_24h").as("val_mean_24h"),
    first("val_mean_120h").as("val_mean_1h"),
    first("val_z_score_1h").as("val_z_score_1h"),
    first("val_z_score_12h").as("val_z_score_12h"),
    first("val_z_score_24h").as("val_z_score_24h"),
    first("val_z_score_120h").as("val_z_score_120h"),
    first("val_ratio_1h").as("val_ratio_1h"),
    first("val_ratio_12h").as("val_ratio_12h"),
    first("val_ratio_24h").as("val_ratio_24h"),
    first("val_ratio_120h").as("val_ratio_120h"),
  )



//
  percentile.join(metrics, percentile("unix_time_p") === metrics("unix_time"))
    .drop("unix_time", "unix_time_p")
    .show(false)

  /*
  result:
+--------+-----+------------------+------------------+------------------+------------------+-----------------+-----------------+-----------------+-----------------+-------------------+-------------------+-------------------+-------------------+-------------------+-------------------+-------------------+-------------------+
|time    |value|val_std_1h        |val_std_12h       |val_std_24h       |val_std_1h        |val_mean_1h      |val_mean_12h     |val_mean_24h     |val_mean_1h      |val_z_score_1h     |val_z_score_12h    |val_z_score_24h    |val_z_score_120h   |val_ratio_1h       |val_ratio_12h      |val_ratio_24h      |val_ratio_120h     |
+--------+-----+------------------+------------------+------------------+------------------+-----------------+-----------------+-----------------+-----------------+-------------------+-------------------+-------------------+-------------------+-------------------+-------------------+-------------------+-------------------+
|13:10:00|386.0|0.0               |0.0               |0.0               |0.0               |386.0            |386.0            |386.0            |386.0            |null               |null               |null               |null               |0.0                |0.0                |0.0                |0.0                |
|13:20:00|100.0|161.1583384889788 |161.1583384889788 |161.1583384889788 |161.1583384889788 |76.59652833491357|76.59652833491357|76.59652833491357|76.59652833491357|-0.4752874040095158|-0.4752874040095158|-0.4752874040095158|-0.4752874040095158|-1.0               |-1.0               |-1.0               |-1.0               |
|13:30:00|100.0|164.71459718876633|164.71459718876633|164.71459718876633|164.71459718876633|72.3747506234414 |72.3747506234414 |72.3747506234414 |72.3747506234414 |-0.4393948797415838|-0.4393948797415838|-0.4393948797415838|-0.4393948797415838|-1.0               |-1.0               |-1.0               |-1.0               |
|13:40:00|100.0|166.5922100848585 |166.5922100848585 |166.5922100848585 |166.5922100848585 |72.6388872329856 |72.6388872329856 |72.6388872329856 |72.6388872329856 |-0.406014706201039 |-0.406014706201039 |-0.406014706201039 |-0.406014706201039 |-0.9870466321243523|-0.9870466321243523|-0.9870466321243523|-0.9870466321243523|
|13:50:00|100.0|165.107283438639  |165.107283438639  |165.107283438639  |165.107283438639  |72.39188962274712|72.39188962274712|72.39188962274712|72.39188962274712|-0.4384536412632037|-0.4384536412632037|-0.4384536412632037|-0.4384536412632037|-1.0               |-1.0               |-1.0               |-1.0               |
|14:00:00|386.0|168.11204956909694|168.11204956909694|168.11204956909694|168.11204956909694|72.84631627266468|72.84631627266468|72.84631627266468|72.84631627266468|-0.4333200175679471|-0.4333200175679471|-0.4333200175679471|-0.4333200175679471|-1.0               |-1.0               |-1.0               |-1.0               |
|14:10:00|392.0|169.98293272934782|170.2473315273081 |170.2473315273081 |170.2473315273081 |73.20687976634756|73.61575524490634|73.61575524490634|73.61575524490634|-0.4306719421232122|-0.432404752453334 |-0.432404752453334 |-0.432404752453334 |null               |-1.0               |-1.0               |-1.0               |
+--------+-----+------------------+------------------+------------------+------------------+-----------------+-----------------+-----------------+-----------------+-------------------+-------------------+-------------------+-------------------+-------------------+-------------------+-------------------+-------------------+
*/

}

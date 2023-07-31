import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import sys.process._
import java.io.File
import java.sql.Timestamp
object sSpark {


  def sparktest1() {
    val warehouseLocation = new File("/home/jana/IdeaProjects/data").getAbsolutePath
    println(warehouseLocation)
    val spark = SparkSession
      .builder()
      .appName("Java Spark SQL basic example")
      .master("yarn")
      .config("spark.master", "local")
      .config("spark.sql.warehouse.dir", "file:"+warehouseLocation)
      //.config("spark.sql.warehouse.dir", "/data/sample")
      //.config("hive.metastore.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse")
      //.enableHiveSupport()
      .getOrCreate();

    import spark.implicits._
    println("Hello World")
    println(spark.version)
    spark.stop()
  }

  def sparktest2() {
    val warehouseLocation = new File("/home/jana/IdeaProjects/data").getAbsolutePath
    println(warehouseLocation)
    val spark = SparkSession
      .builder()
      .appName("Scala Spark SQL basic example")
      .master("yarn")
      .config("spark.master", "local")
      .config("spark.sql.warehouse.dir", "file:" + warehouseLocation)
      .getOrCreate()

    import spark.implicits._
    val schPer = new StructType(Array(
      new StructField("Column1", IntegerType, false),
      new StructField("Column2", StringType, true),
      new StructField("Column3", StringType, true),
      new StructField("Column4", IntegerType, true)
    ))

    val dftest2 = spark.read.format("csv")
      .option("header", false)
      .schema(schPer)
      .option("timestampFormat", "M/d/yy:H:m:s")
      .load("../data/src_test2.csv")


    dftest2.coalesce(1).write.format("csv").save("/home/jana/IdeaProjects/data/test2_out.csv")
    //spark.sql("show tables").show()
    spark.stop()
  }

  case class taxi(vendor_name: String, Trip_Pickup_DateTime: String,
                  Trip_Dropoff_DateTime: String, Passenger_Count: String, Trip_Distance: String,
                  Start_Lon: String, Start_Lat: String, Rate_Code: String, store_and_forward: String, End_Lon: String, End_Lat: String,
                  Payment_Type: String, Fare_Amt: String, surcharge: String, mta_tax: String, Tip_Amt: String, Tolls_Amt: String, Total_Amt: String
                 )
  case class zipc(State_name: String, State_abbr: String)


  def sparktest3() {
    val schema =
      StructType(
        Array(
          StructField("vendor_name", StringType, nullable = false),
          StructField("Trip_Pickup_DateTime", StringType, nullable = false),
          StructField("Trip_Dropoff_DateTime", StringType, nullable = false),
          StructField("Passenger_Count", StringType, nullable = false),
          StructField("Trip_Distance", StringType, nullable = false),
          StructField("Start_Lon", StringType, nullable = false),
          StructField("Start_Lat", StringType, nullable = false),
          StructField("Rate_Code", StringType, nullable = false),
          StructField("store_and_forward", StringType, nullable = false),
          StructField("End_Lon", StringType, nullable = false),
          StructField("End_Lat", StringType, nullable = false),
          StructField("Payment_Type", StringType, nullable = false),
          StructField("Fare_Amt", StringType, nullable = false),
          StructField("surcharge", StringType, nullable = false),
          StructField("mta_tax", StringType, nullable = false),
          StructField("Tip_Amt", StringType, nullable = false),
          StructField("Tolls_Amt", StringType, nullable = false),
          StructField("Total_Amt", StringType, nullable = false)
        )
      )
    val spark = SparkSession
      .builder()
      .appName("Java Spark SQL basic example")
      .master("yarn")
      .config("spark.master", "local")
      .config("spark.sql.warehouse.dir", "file:/home/jana/IdeaProjects/data")
      //.config("spark.sql.warehouse.dir", "/data/sample")
      //.config("hive.metastore.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse")
      //.enableHiveSupport()
      .getOrCreate();

    import spark.implicits._
    val ds = spark.read
      .schema(schema)
      .option("timestampFormat", "M/d/yy:H:m:s")
      .csv("./taxi-etl-input-small.csv")
      .as[sSpark.taxi]

    ds.collect.foreach(println)
    //spark.sql("show tables").show()
    spark.stop()
  }


  def sparkHiveEx1() {
    //val jarlist = Seq("/bin/sh", "-c", "echo /opt/spark-3.2.2-bin-hadoop3.2/jars/*jar | tr ' ' ','").!!
    //println(jarlist)
    val spark = SparkSession
      .builder()
      .appName("Scala Spark with MySQL")
      .master("yarn")
      .config("spark.master", "local")
      .config("spark.sql.warehouse.dir", "file:/home/jana/IdeaProjects/data")
      //.config("spark.sql.warehouse.dir", "/data/sample")
      //.config("hive.metastore.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate();
    import spark.implicits._
    Class.forName("com.mysql.cj.jdbc.Driver")
    spark.sql("show databases").show()
    spark.stop()
  }

  def sparkMysql1() {

    val spark = SparkSession
      .builder()
      .appName("Scala Spark with MySQL")
      .master("yarn")
      .config("spark.master", "local")
      .config("spark.sql.warehouse.dir", "file:/home/jana/IdeaProjects/data")
      .enableHiveSupport()
      .getOrCreate();

    import spark.implicits._
    Class.forName("com.mysql.cj.jdbc.Driver")
    val jdbcDF = spark.read
            .format("jdbc")
            .option("driver", "com.mysql.cj.jdbc.Driver")
            .option("url", "jdbc:mysql://localhost:3306/sampleDB")
            //.option("dbtable", "emp")
            .option("query", "Select * from emp where age > 20")
            .option("numPartitions", 5)
            .option("fetchsize", 20)
            .option("user", "root")
            .option("password", "kris")
            .load()
    //val jdbcDF = spark.sqlContext.load("jdbc", Map("url" -> "jdbc:mysql://localhost:3306/sampleDB?user=root&password=kris", "dbtable" -> "emp"))
    jdbcDF.collect.foreach(println)
    //spark.sql("show databases").show()
    spark.stop()
  }

  def sparktstDF1() {

    val spark = SparkSession
      .builder()
      .appName("Scala Spark with MySQL")
      .master("yarn")
      .config("spark.master", "local")
      .config("spark.sql.warehouse.dir", "file:/home/jana/IdeaProjects/data")
      .enableHiveSupport()
      .getOrCreate();

    import spark.implicits._
    val empDF = spark.sqlContext.load("jdbc", Map("url" -> "jdbc:mysql://localhost:3306/sampleDB?user=root&password=kris", "dbtable" -> "emp"))
    empDF.select("name").show()
    empDF.select($"name", $"age" + 1).show()
    empDF.show()
    empDF.printSchema()
    empDF.filter($"age" > 21).show()
    empDF.groupBy("age").count().show()
    //empDF.createOrReplaceTempView("employee")
    //spark.sql("SELECT * FROM employee").show()
    spark.stop()
  }

  def sparkAllDF(): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Scala Spark with MySQL")
      .master("yarn")
      .config("spark.master", "local")
      .config("spark.sql.warehouse.dir", "file:/home/jana/IdeaProjects/data")
      .enableHiveSupport()
      .getOrCreate();

    import spark.implicits._
    //adding new column on existing DF
    val sourceDF = Seq(
      ("quibdo"),
      ("manizales")
    ).toDF("city")

    val actualDF = sourceDF.withColumn(
      "country",
      lit("colombia")
    )
    actualDF.show()
    //Adding 2 columns into third column sum
    val DF1 = Seq(
      (1, 3),
      (3, 3),
      (5, 3)
    ).toDF("num1", "num2")

    val DF1A = DF1.withColumn(
      "sum",
      col("num1") + col("num2")
    )
    DF1A.show()

    //isin function
    val primaryColors = List("red", "yellow", "blue")

    val cDF = Seq(
      ("rihanna", "red"),
      ("solange", "yellow"),
      ("selena", "purple")
    ).toDF("celebrity", "color")

    val cDFA = cDF.withColumn(
      "is_primary_color",
      col("color").isin(primaryColors: _*)
    )
    cDFA.show()

    //usage of when
    val ageDF = Seq(
      (5),
      (14),
      (19),
      (75)
    ).toDF("age")

    val ageDFA = ageDF.withColumn(
      "age_category",
      when(col("age").between(13, 19), "teenager").otherwise(
        when(col("age") <= 7, "young child").otherwise(
          when(col("age") > 65, "elderly")
        )
      )
    )

    ageDFA.show()


    val celDF = Seq(
      ("britney spears", "Mississippi"),
      ("romeo santos", "New York"),
      ("miley cyrus", "Tennessee"),
      ("random dude", null),
      (null, "Dubai")
    ).toDF("name", "birth_state")

    val stAbbrSchema =
      StructType(
        Array(
          StructField("State_name", StringType, nullable = false),
          StructField("State_abbr", StringType, nullable = false)
        )
      )

    import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
    import org.apache.spark.sql.Encoder
    import spark.implicits._
    val stDF = spark
      .read
      .schema(stAbbrSchema)
      .option("header", "true")
      .option("charset", "UTF8")
      .csv("file:///home/jana/Documents/SPARK/examples/state_abbreviation.csv")

    stDF.show()
    stDF.printSchema()

    val stateMappingsDF = spark
      .read
      .schema(stAbbrSchema)
      .option("header", "true")
      .option("charset", "UTF8")
      .csv("file:///home/jana/Documents/SPARK/examples/state_abbreviation.csv")
      .as[zipc]
    stateMappingsDF.show()
    stateMappingsDF.printSchema()

    val stateMapDF = spark.sparkContext.textFile("file:///home/jana/Documents/SPARK/examples/state_abbreviation.csv").map(_.split(","))
        .map(attributes => zipc(attributes(0).trim, attributes(1).trim)).toDF()
    stateMapDF.show()

    stateMappingsDF.write.format("csv").mode("overwrite").save("file:///home/jana/IdeaProjects/data/result_csv")

    //custom function on data frames:
    val funDF = Seq(
      ("surfing"),
      ("dancing")
    ).toDF("activity")

    def withGreeting()(df: org.apache.spark.sql.DataFrame): org.apache.spark.sql.DataFrame = {
      df.withColumn("greeting", lit("hello world"))
    }

    val funDFA = withGreeting()(funDF)
    funDFA.show()

    //Overwriting existing columns using withColumn():
    val oDF = Seq(
      ("weird al"),
      ("chris rock")
    ).toDF("person")

    val oDFA = oDF.withColumn(
      "person",
      concat(col("person"), lit(" is funny"))
    )
    oDFA.show()


    //spark.sparkContext.textFile("file:///home/jana/Documents/SPARK/examples/state_abbreviation.csv").map(_.split(",")).foreach{x => x.foreach { y => println(y) } }
    spark.stop()

  }

  def sparkMySqlWrite(): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Scala Spark with MySQL")
      .master("yarn")
      .config("spark.master", "local")
      .config("spark.sql.warehouse.dir", "file:/home/jana/IdeaProjects/data")
      .enableHiveSupport()
      .getOrCreate();

    val stAbbrSchema =
      StructType(
        Array(
          StructField("State_name", StringType, nullable = false),
          StructField("state_abbr", StringType, nullable = false)
        )
      )

    import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
    import org.apache.spark.sql.Encoder
    import spark.implicits._
//    val stDF = spark
//      .read
//      .schema(stAbbrSchema)
//      .option("header", "true")
//      .option("charset", "UTF8")
//      .csv("file:///home/jana/Documents/SPARK/examples/state_abbreviation.csv")

    val stateMappingsDF = spark
      .read
      .schema(stAbbrSchema)
      .option("header", "true")
      .option("charset", "UTF8")
      .csv("file:///home/jana/Documents/SPARK/examples/state_abbreviation.csv")
      //.as[zipc]
    stateMappingsDF.show()
    stateMappingsDF.printSchema()

    //stateMappingsDF.write.format("csv").save("file:///home/jana/IdeaProjects/data/result_csv")

    stateMappingsDF.write
      .format("jdbc")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("url", "jdbc:mysql://localhost:3306/sampleDB")
      .option("dbtable", "stateMapping")
      .option("user", "root")
      .option("password", "kris")
      .mode("append")
      .save()

    //spark.sparkContext.textFile("file:///home/jana/Documents/SPARK/examples/state_abbreviation.csv").map(_.split(",")).foreach{x => x.foreach { y => println(y) } }
    spark.stop()

  }

  def sparkJoin1(): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Scala Spark with MySQL")
      .master("yarn")
      .config("spark.master", "local")
      .config("spark.sql.warehouse.dir", "file:/home/jana/IdeaProjects/data")
      .enableHiveSupport()
      .getOrCreate();

    val stAbbrSchema =
      StructType(
        Array(
          StructField("State_name", StringType, nullable = false),
          StructField("state_abbr", StringType, nullable = false)
        )
      )

    import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
    import org.apache.spark.sql.Encoder
    import spark.implicits._
    val stateMappingsDF = spark
      .read
      .schema(stAbbrSchema)
      .option("header", "true")
      .option("charset", "UTF8")
      .csv("file:///home/jana/Documents/SPARK/examples/state_abbreviation.csv")
    //.as[zipc]
    stateMappingsDF.show()
    stateMappingsDF.printSchema()

    stateMappingsDF.write.format("csv")
      .mode("overwrite")
      .save("file:///home/jana/IdeaProjects/data/result_csv")


    val celDF = Seq(
      ("britney spears", "Mississippi"),
      ("romeo santos", "New York"),
      ("miley cyrus", "Tennessee"),
      ("random dude", null),
      (null, "Dubai")
    ).toDF("name", "birth_state")

    val resultDF = celDF.join(broadcast(stateMappingsDF), celDF("birth_state") <=> stateMappingsDF("state_name"), "left_outer").drop(stateMappingsDF("state_name"))
    resultDF.show()



    spark.stop()

  }


  //Test to check the class files are available
// import java.sql.{DriverManager, Connection}
//
//  private var driverLoaded = false
//
//  private def loadDriver() {
//    try {
//      //
//      // Class.forName("com.mysql.jdbc.Driver").newInstance
//      Class.forName("com.mysql.cj.jdbc.Driver").newInstance
//      driverLoaded = true
//    } catch {
//      case e: Exception => {
//        println("ERROR: Driver not available: " + e.getMessage)
//        throw e
//      }
//    }
//  }
  def main(args: Array[String]): Unit = {
    println("Hello Spark world!")
    //sparktest1()
    //sparktest2()
    //sparktest3()
    //sparkHiveEx1()
    //sparkHiveEx1()
    //sparkMysql1()
    //loadDriver()
    //sparktstDF1()
    sparkAllDF()
    //sparkMySqlWrite()
    //sparkJoin1()
  }
}

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

  def MysqlReadWrite() {

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
    val empDF = spark.read
            .format("jdbc")
            .option("driver", "com.mysql.cj.jdbc.Driver")
            .option("url", "jdbc:mysql://localhost:3306/sampleDB")
            //.option("dbtable", "emp")
            .option("query", "Select * from emp")
            .option("numPartitions", 5)
            .option("fetchsize", 20)
            .option("user", "root")
            .option("password", "kris")
            .load()
    //val jdbcDF = spark.sqlContext.load("jdbc", Map("url" -> "jdbc:mysql://localhost:3306/sampleDB?user=root&password=kris", "dbtable" -> "emp"))
    empDF.collect.foreach(println)
    empDF.select("name").show()
    empDF.select($"name", $"age" + 1).show()
    empDF.show()
    empDF.printSchema()
    empDF.filter($"age" > 21).show()
    empDF.groupBy("age").count().show()


    empDF.write
      .format("jdbc")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("url", "jdbc:mysql://localhost:3306/sampleDB")
      .option("dbtable", "empNew")
      .option("user", "root")
      .option("password", "kris")
      .mode("append")
      .save()

    //spark.sql("show databases").show()
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

  def FileReadMySqlWrite(): Unit = {
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


  def FileReadFileWrite(): Unit = {
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
    //hdfs dfs -rm -r -f file:///home/jana/IdeaProjects/data/sample_zip
    stateMappingsDF.write.format("csv").mode("overwrite").save("file:///home/jana/IdeaProjects/data/sample_zip/result_csv")

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

  def sparkJoin2(): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Scala Spark with Join")
      .master("yarn")
      .config("spark.master", "local")
      .config("spark.sql.warehouse.dir", "file:/home/jana/IdeaProjects/data")
      .enableHiveSupport()
      .getOrCreate();

    import org.apache.spark.sql.catalyst.plans.{LeftOuter, Inner}
    import spark.sqlContext.implicits._

    val emp = Seq((1, "Smith", -1, "2018", "10", "M", 3000),
      (2, "Rose", 1, "2010", "20", "M", 4000),
      (3, "Williams", 1, "2010", "10", "M", 1000),
      (4, "Jones", 2, "2005", "10", "F", 2000),
      (5, "Brown", 2, "2010", "40", "", -1),
      (6, "Brown", 2, "2010", "50", "", -1)
    )

    val empColumns = Seq("emp_id", "name", "superior_emp_id", "year_joined",
      "emp_dept_id", "gender", "salary")

    val empDF = emp.toDF(empColumns: _*)
    empDF.show(false)
    empDF.printSchema()


    val dept = Seq(("Finance", 10),
      ("Marketing", 20),
      ("Sales", 30),
      ("IT", 40)
    )

    val deptColumns = Seq("dept_name", "dept_id")
    val deptDF = dept.toDF(deptColumns: _*)
    deptDF.show(false)
    deptDF.printSchema()

    //Inner:
    empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "inner").show(false)

    //Outer:
    empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "outer").show(false)
    empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "full").show(false)
    empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "fullouter").show(false)

    //Left Outer:
    empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "left").show(false)
    empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "leftouter").show(false)

    //Rght Outer:
    empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "right").show(false)
    empDF.join(deptDF, empDF("emp_dept_id") === deptDF("dept_id"), "rightouter").show(false)

    //left semi:
    empDF.join(deptDF,empDF("emp_dept_id") ===  deptDF("dept_id"),"leftsemi").show(false)

    //left anti:
    empDF.join(deptDF,empDF("emp_dept_id") ===  deptDF("dept_id"),"leftanti").show(false)

    //self join:
//    empDF.as("emp1").join(empDF.as("emp2"),
//        col("emp1.superior_emp_id") === col("emp2.emp_id"), "inner")
//      .select(col("emp1.emp_id"), col("emp1.name"),
//        col("emp2.emp_id").as("superior_emp_id"),
//        col("emp2.name").as("superior_emp_name"))
//      .show(false)

    empDF.as("emp1").join(empDF.as("emp2"), col("emp1.superior_emp_id") === col("emp2.emp_id"), "inner")
      .select(col("emp1.emp_id"), col("emp1.name"),
        col("emp2.emp_id").as("superior_id"),
        col("emp2.name").as("superior_name"))
      .show()

    empDF.createOrReplaceTempView("EMP")
    deptDF.createOrReplaceTempView("DEPT")

    val joinDF = spark.sql("select * from EMP e, DEPT d where e.emp_dept_id == d.dept_id")
    joinDF.show(false)
    val joinDF2 = spark.sql("select * from EMP e LEFT ANTI JOIN DEPT d ON e.emp_dept_id == d.dept_id")
    joinDF2.show(false)

    val dfPeople = spark.read.json("file:///home/jana/Documents/SPARK/examples/people.json")
    dfPeople.show()


    val simpleData = Seq(("James", "Sales", "NY", 90000, 34, 10000),
      ("Michael", "Sales", "NY", 86000, 56, 20000),
      ("Robert", "Sales", "CA", 81000, 30, 23000),
      ("Maria", "Finance", "CA", 90000, 24, 23000),
      ("Raman", "Finance", "CA", 99000, 40, 24000),
      ("Scott", "Finance", "NY", 83000, 36, 19000),
      ("Jen", "Finance", "NY", 79000, 53, 15000),
      ("Jeff", "Marketing", "CA", 80000, 25, 18000),
      ("Kumar", "Marketing", "NY", 91000, 50, 21000)
    )
    val df = simpleData.toDF("employee_name", "department", "state", "salary", "age", "bonus")
    df.show()

    df.groupBy("department").sum("salary").show(false)
    df.groupBy("department").count()
    df.groupBy("department").min("salary")
    df.groupBy("department").avg("salary").show()
    df.groupBy("department").mean("salary").show()

    //GroupBy on multiple columns
    df.groupBy("department", "state")
      .sum("salary", "bonus")
      .show(false)

    //Running more aggregates at a time
    import org.apache.spark.sql.functions._
    df.groupBy("department")
      .agg(
        sum("salary").as("sum_salary"),
        avg("salary").as("avg_salary"),
        sum("bonus").as("sum_bonus"),
        max("bonus").as("max_bonus"))
      .show(false)

    //Using filter on aggregate data (having)

    df.groupBy("department")
      .agg(
        sum("salary").as("sum_salary"),
        avg("salary").as("avg_salary"),
        sum("bonus").as("sum_bonus"),
        max("bonus").as("max_bonus"))
      .where(col("sum_bonus") >= 50000)
      .show(false)


    val data = Seq(("Banana", 1000, "USA"), ("Carrots", 1500, "USA"), ("Beans", 1600, "USA"),
      ("Orange", 2000, "USA"), ("Orange", 2000, "USA"), ("Banana", 400, "China"),
      ("Carrots", 1200, "China"), ("Beans", 1500, "China"), ("Orange", 4000, "China"),
      ("Banana", 2000, "Canada"), ("Carrots", 2000, "Canada"), ("Beans", 2000, "Mexico"))

    import spark.sqlContext.implicits._
    val dfFruit = data.toDF("Product", "Amount", "Country")
    dfFruit.show()

    val prod = Seq("Product", "Amount", "Country")
    val dfProd = data.toDF(prod: _*)

    //This will transpose the countries from rows into columns and produces below output. Where ever data is not present, it represents as null by default.
    val pivotDF = dfProd.groupBy("Product").pivot("Country").sum("Amount")
    pivotDF.show()

    //same pivot with required column order
    val countries = Seq("USA","China","Canada","Mexico")
    val pcDF = dfProd.groupBy("Product").pivot("Country", countries).sum("Amount")
    pcDF.show()

    //same with better performance
    val betPivotDF = dfProd.groupBy("Product", "Country")
      .sum("Amount")
      .groupBy("Product")
      .pivot("Country")
      .sum("sum(Amount)")
    betPivotDF.show()

    //unpivot
    //It converts pivoted colum "country" to rows
    val unPivotDF = pivotDF.select($"Product", expr("stack(3, 'Canada', Canada, 'China', China, 'Mexico', Mexico) as (Country,Total)")).where("Total is not null")
    unPivotDF.show()
    val unPivotAllDF = pivotDF.select($"Product", expr("stack(4, 'Canada', Canada, 'China', China, 'Mexico', Mexico, 'USA', USA) as (Country,Total)"))
      .where("Total is not null")
    unPivotAllDF.show()

    //Transpose:
    val dfc = Seq(
      ("col1", "val1"),
      ("col2", "val2"),
      ("col3", "val3"),
      ("col4", "val4"),
      ("col5", "val5")
    ).toDF("COLUMN_NAME", "VALUE")
    dfc
      .groupBy()
      .pivot("COLUMN_NAME").agg(first("VALUE"))
      .show()



    unPivotAllDF.write.mode(SaveMode.Overwrite)
      .saveAsTable("test.prodSales")
    // Create Hive Internal table
    //unPivotAllDF.write.mode(SaveMode.Overwrite)
    //  .saveAsTable("test.productSales")

    spark.stop()

  }

  def HiveRead(): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Scala Spark with Join")
      .master("yarn")
      .config("spark.master", "local")
      .config("spark.sql.warehouse.dir", "file:/home/jana/IdeaProjects/data")
      .enableHiveSupport()
      .getOrCreate();

    import spark.sqlContext.implicits._
    val dfemp = spark.sql("Select * from test.employee")
    dfemp.show(false)
    dfemp.printSchema()
    spark.stop()


  }

  def HiveReadWrite(): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Scala Spark with Join")
      .master("yarn")
      .config("spark.master", "local")
      .config("spark.sql.warehouse.dir", "file:/home/jana/IdeaProjects/data")
      .enableHiveSupport()
      .getOrCreate();

    val dfemp = spark.sql("Select * from test.employee")
    dfemp.show(false)
    dfemp.printSchema()

    val data = Seq(("Banana", 1000, "USA"), ("Carrots", 1500, "USA"), ("Beans", 1600, "USA"),
      ("Orange", 2000, "USA"), ("Orange", 2000, "USA"), ("Banana", 400, "China"),
      ("Carrots", 1200, "China"), ("Beans", 1500, "China"), ("Orange", 4000, "China"),
      ("Banana", 2000, "Canada"), ("Carrots", 2000, "Canada"), ("Beans", 2000, "Mexico"))

    import spark.sqlContext.implicits._
    val prod = Seq("Product", "Amount", "Country")
    val dfProd = data.toDF(prod: _*)

    //same pivot with required column order
    val countries = Seq("USA", "China", "Canada", "Mexico")
    val pivotDF = dfProd.groupBy("Product").pivot("Country", countries).sum("Amount")
    pivotDF.show()

    //same with better performance
    val betPivotDF = dfProd.groupBy("Product", "Country")
      .sum("Amount")
      .groupBy("Product")
      .pivot("Country")
      .sum("sum(Amount)")
    betPivotDF.show()


    //unpivot
    //It converts pivoted colum "country" to rows
    val unPivotAllDF = pivotDF.select($"Product", expr("stack(4, 'Canada', Canada, 'China', China, 'Mexico', Mexico, 'USA', USA) as (Country,Total)"))
      .where("Total is not null")
    unPivotAllDF.show()

    // Create database
    spark.sql("CREATE DATABASE IF NOT EXISTS test")

    unPivotAllDF.write.mode(SaveMode.Overwrite)
      .saveAsTable("test.prodSales")

    // Create Hive External table
    //hdfs dfs -rm -r -f file:/home/jana/IdeaProjects/data/sample
    unPivotAllDF.write.mode(SaveMode.Overwrite)
      .option("path", "/home/jana/IdeaProjects/data/sample")
      .saveAsTable("test.sales")

    import spark.implicits._
    unPivotAllDF.write.format("csv").mode("overwrite").save("file:///home/jana/IdeaProjects/data/sample_prod/prod_sales")

    spark.stop()


  }



  def main(args: Array[String]): Unit = {
    println("Hello Spark world!")
    //sparktest1()
    //sparktest2()
    //sparktest3()
    //sparkHiveEx1()

    //MysqlReadWrite()
    //FileReadMySqlWrite()
    //FileReadFileWrite()

    //sparkAllDF()
    //sparkJoin1()
    //sparkJoin2()

    //HiveRead()
   HiveReadWrite()
  }
}

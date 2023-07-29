import org.saddle.io._

import java.text.{DateFormat, FieldPosition, Format, ParsePosition, SimpleDateFormat}
import java.util.Date
//import java.awt._
import java.io._
import java.io.File
//import java.util.{Map, HashMap}
import scala.collection.mutable.ArrayBuffer

import sys.process._


object Main {
  println("Month, Income, Expenses, Profit")

  def csvlayout(): Unit = {
    val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
    val tmformat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    case class taxi (vendor_name: String, Trip_Pickup_DateTime: String,
                     Trip_Dropoff_DateTime: String, Passenger_Count: String, Trip_Distance: String,
                     Start_Lon: String, Start_Lat: String, Rate_Code: String, store_and_forward: String, End_Lon: String, End_Lat: String,
                     Payment_Type: String, Fare_Amt: Float, surcharge: Float) /*, mta_tax: Float, Tip_Amt: Float, Tolls_Amt: Float, Total_Amt: Float
                     ) */
    val rows = ArrayBuffer[Array[String]]()
    val source = io.Source.fromFile("taxi-etl-input-small.csv")
    for (line <- source.getLines.drop(1)) {
      rows += line.split(",").map(_.trim)
      //val cols = line.split(",").map(_.trim)
      //line.split(",")(0).map(r => taxi(r.toChar))
      val llaxi = line.split(",").map(_.trim).toList match
      { case a :: b :: c :: d :: e :: f :: g :: h :: i :: j :: k :: l :: m :: n :: o :: p :: q :: r :: Nil => taxi(a.toString, b.toString, c.toString, d.toString, e.toString,
          f.toString, g.toString, h.toString, i.toString, j.toString, k.toString, l.toString, m.toFloat, n.toFloat) //, o.toFloat, p.toFloat, q.toFloat, r.toFloat)
        case _ => println("Not valid Line") }
      println(llaxi)

      /*, r(1).toString, r(2).toString, r(3).toString, r(4).toString, r(5).toString,
            r(6).toString, r(7).toString, r(8).toString, r(9).toString, r(10).toString, r(11).toString,
            r(12).toFloat, r(13).toFloat, r(14).toFloat, r(15).toFloat, r(16).toFloat, r(17).toFloat)))*/

    }
    source.close
    // (2) print the results
    for (row <- rows) {
      //println(s"${row.length}")
    }
  }
  def filecsv(): Unit = {
    // each row is an array of strings (the columns in the csv file)
    val rows = ArrayBuffer[Array[String]]()
    try {
      val source = io.Source.fromFile("taxi-etl-input-small.csv")
      for (line <- source.getLines.drop(1)) {
        val cols = line.split(",").map(_.trim)
        // do whatever you want with the columns here
        //println(s"${cols(0)}|${cols(1)}|${cols(2)}|${cols(3)}")
        rows += line.split(",").map(_.trim)
        source.close
      }
    } catch {
      case e: FileNotFoundException => println("Couldn't find that file.")
      case e: IOException => println("Had an IOException trying to read that file")
    }
    finally {
      // your scala code here, such as closing a database connection
      // or file handle
    }


    // (2) print the results
    for (row <- rows) {
      println(s"${row(0)}|${row(1)}|${row(2)}|${row(3)}")
    }

    println(System.getProperty("user.dir"))
    /*val file = CsvFile("taxi-small.csv")
    val df = CsvParser.parse(file).withColIndex(0)
    println(df)
*/
  }

  def saddlesample(): Unit = {
    val file = CsvFile("taxi-etl-input-small.csv")
    val df = CsvParser.parse(file).withRowIndex(0).withColIndex(0)
    println(df)
    val df2 = df.rfilterIx {case x => x == "VTS" }
    println(df2)

    /*
        val wkg = df2.col("Weight").mapValues(CsvParser.parseDouble).
          mapValues(_ * 0.453592).setColIndex(Index("WeightKG"))
        val df3 = df2.joinPreserveColIx(wkg.mapValues(_.toString))
        println(df3)
        df3.writeCsvFile("saddle-out.csv")
    */


  }
  def getListOfFiles(dir: String): List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  def getListOfFilesWfilter(dir: File, extensions: List[String]): List[File] = {
    dir.listFiles.filter(_.isFile).toList.filter { file =>
      extensions.exists(file.getName.endsWith(_))
    }
  }

  // assumes that dir is a directory known to exist
  def getListOfSubDirectories(dir: File): List[String] =
    dir.listFiles
      .filter(_.isDirectory)
      .map(_.getName)
      .toList
  def sampleMap(): Unit = {
    val ratings = Map(
      "Lady in the Water" -> 3.0,
      "Snakes on a Plane" -> 4.0,
      "You, Me and Dupree" -> 3.5
    )
    for ((name,rating) <- ratings) println(s"Movie: $name, Rating: $rating")
    ratings.foreach {
      case (movie, rating) => println(s"key: $movie, value: $rating")
    }
  }
  def tstYield(): Unit = {
    val names = List("_adam", "_david", "_frank")
    val capNames = for (name <- names) yield {
      val nameWithoutUnderscore = name.drop(1)
      val capName = nameWithoutUnderscore.capitalize
      capName
    }

    val capNamess = for (name <- names) yield name.drop(1).capitalize
    capNamess.foreach(println)
  }
  def tstMatch(i: Int): Unit = {
    val monthName = i match {
      case 1 => "January"
      case 2 => "February"
      case 3 => "March"
      case 4 => "April"
      case 5 => "May"
      case 6 => "June"
      case 7 => "July"
      case 8 => "August"
      case 9 => "September"
      case 10 => "October"
      case 11 => "November"
      case 12 => "December"
      case _ => "Invalid month"
    }
    println(monthName)

    val evenOrOdd = i match {
      case 1 | 3 | 5 | 7 | 9 => println("odd")
      case 2 | 4 | 6 | 8 | 10 => println("even")
      case _ => println("some other number")
    }
  }
  def main(args: Array[String]): Unit = {
    println("Hello world!")
    //filecsv()
    //csvlayout()
    saddlesample()
    //var files = getListOfFiles("../.")
    //files.foreach(println)

    //val okFileExtensions = List("wav", "pdf")
    //files = getListOfFilesWfilter(new File("../."), okFileExtensions)
    //files.foreach(println)

    //getListOfSubDirectories(new File("../.")).foreach(println)
    //sampleMap()
    //tstYield()

    //tstMatch(5)
  }

}
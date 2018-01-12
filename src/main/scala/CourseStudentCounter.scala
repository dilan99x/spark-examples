import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

/**
  *
  */
object CourseStudentCounter {
  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine, named CourseCounter
    val sc = new SparkContext("local[*]", "CourseCounter")

    // Load up each line of the ratings data into an RDD
    val lines = sc.textFile("/home/dilan/Desktop/Workshop/DataSet/student_data")


    val courseCounts = lines.map(x => ((x.toString().split(",")(6),x.toString().split(",")(2), x)))

    val filteredData = courseCounts.filter(y => y._1.trim().equalsIgnoreCase("Medicine") && y._2.toInt < 25).map(z => z._3)

    filteredData.foreach(println)

    sc.stop()

  }
}

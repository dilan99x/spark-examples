import org.apache.spark.SparkContext
import org.apache.log4j._

/**
  * Display students those who are younger than 20 years of age
  *
  */
object CountAgeCategory {
  /** main function where the action happens */
  def main(args: Array[String]): Unit = {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine, named CourseCounter
    val sc = new SparkContext("local[*]", "AgeFilter")

    // Load up each line of the ratings data into an RDD
    val lines = sc.textFile("/home/user/DataSet/student_data")
    /* Convert each line to a string, split it out by comma (,)  and extract the third field.
      * (The file format is first name, last name, age, continent, country, education level, selected course)
      */
    val ageRDD = lines.map(x => ((x.toString().split(",")(2), x)))
    /*
     * Applying filter to the first element of the tuple and filtering data with my condition which is < 20
     * and after fetching send element of the tuple returned from the above filter operation 
     */
    val filteredData = ageRDD.filter(y => y._1.trim().toInt < 20).map(z => z._2)

    //Print down each line of the map
    filteredData.foreach(println)

    //stooping the SparkContext
    sc.stop()
  }
}
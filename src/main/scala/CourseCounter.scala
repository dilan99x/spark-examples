import org.apache.spark.SparkContext

/**
  * This will count number of students registered for each course
  */
object CourseCounter {

  /** main function where the action happens */
  def main(args: Array[String]): Unit = {
    // Create a SparkContext using every core of the local machine, named CourseCounter
    val sparkContext = new SparkContext("local[*]", "CourseCounter")

    // Load up each line of the ratings data into an RDD
    val lines = sparkContext.textFile("/home/dilan/Desktop/Workshop/DataSet/student_data")

    // Caching the student data in the cache store
    lines.cache()

    val ratings = lines.map(x => (x.toString().split(",")(6)))

    // Count up how many times each value (course) occurs
    val results = ratings.countByValue()

    // Print each result on its own line.
    results.foreach(println)

    //stopping the SparkContext
    sparkContext.stop()
  }
}

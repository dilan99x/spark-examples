import org.apache.spark.SparkContext

/**
  * This will find  which country represent the most number of students enrollment for the courses
  */
object MostStudentsCountryCalculator {

  def main(args: Array[String]): Unit = {
    // Create a SparkContext using every core of the local machine, named CourseCounter
    val sc = new SparkContext("local[*]", "CourseCounter")

    // Load up each line of the ratings data into an RDD
    val lines = sc.textFile("/home/dilan/Desktop/Workshop/DataSet/student_data")

    // Caching the student data in the cache store
    lines.cache()

    // Finding the most number of student enrolments in terms of country
    val result = lines.map(x => (x.toString().split(",")(4))).countByValue().reduce((a, b) => if (a._2 > b._2) a else b)

    // Printing the result.
    println(result)

    //stopping the SparkContext
    sc.stop()
  }

}


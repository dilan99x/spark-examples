import org.apache.spark.SparkContext

/**
  * This will display first name and the last name of all the students
  */
object ListStudents {

  /** main function where the action happens */
  def main(args: Array[String]): Unit = {
    // Create a SparkContext using every core of the local machine, named CourseCounter
    val sc = new SparkContext("local[*]", "StudentsCounter")

    // Load up each line of the ratings data into an RDD
    val lines = sc.textFile("/home/user/DataSet/student_data")

    //Extracting the first name and the last name
    val names = lines.map(x => (x.toString().split(",")(0), x.toString().split(",")(1)))

    // Print each result on its own line.
    names.foreach(println)

    //shutting down the SparkContext
    sc.stop();
  }
}

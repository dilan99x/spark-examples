import org.apache.spark.SparkContext
import org.apache.log4j._

/**
  * RDD join example
  */
object StudentQualificationsJoin {

  def main(args: Array[String]) {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine, named Joiner
    val sc = new SparkContext("local[*]", "Joiner")

    // Reading student data from the text file
    val studentRdd = sc.textFile("/home/user/DataSet/student_join_data")

    // Reading education qualification data from the text file
    val educationQlfRdd = sc.textFile("/home/user/DataSet/student_course")

    // Making tuple with student id and rest of the student details
    val students = studentRdd.map(x => (x.toString().split(",")(0), x))

    // Making the tuple with student id and the education qualification
    val qualifications = educationQlfRdd.map(x => (x.toString().split(",")(0), x.toString().split(",")(1)))

    //Joining the two RDDs
    val collection = students.join(qualifications).map(z => z._2)

    //Listing the results
    collection.foreach(println)

    //stopping the SparkContext
    sc.stop()
  }
}
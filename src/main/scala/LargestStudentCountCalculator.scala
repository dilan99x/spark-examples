import org.apache.spark.SparkContext

/**
  *
  */
object LargestStudentCountCalculator {

  def main(args: Array[String]): Unit = {
    // Create a SparkContext using every core of the local machine, named CourseCounter
    val sc = new SparkContext("local[*]", "CourseCounter")

    // Load up each line of the ratings data into an RDD
    val lines = sc.textFile("/home/user/DataSet/student_data")
    lines.cache()

    val ratings = lines.map(x => (x.toString().split(",")(6)))

    // Count up how many times each value (course) occurs
    val results = ratings.countByValue().reduce((x,y)=> if(x._2 > y._2) x else y)

    println(results)




    sc.stop()

  }

}

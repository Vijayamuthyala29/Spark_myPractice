package SparkJson_Maven_project.SparkJson_Maven_project
import org.apache.spark.sql.SparkSession
object readJsonData {
  
  def main(args:Array[String]){
    
    var spark=SparkSession.builder().appName("Json Read").master("local").getOrCreate()
    println(spark)
    val input_path="C:\\Users\\Vijayashanthi\\Desktop\\BigdataCourse\\athlete_events.csv"
    var readCsvDf=spark.read.format("csv").option("header","true").csv(input_path)
    //println(readCsvDf.show)
    var count1=readCsvDf.count()
    println("original count="+count1)
     var dupDf=readCsvDf.dropDuplicates()
    // var count2=dupDf.count()
   // println("after duplicates count="+count2)
    
    
  }
  
}
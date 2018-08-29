import com.amazonaws.services.glue.ChoiceOption
import com.amazonaws.services.glue.DynamicFrame
import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.MappingSpec
import com.amazonaws.services.glue.ResolveSpec
import com.amazonaws.services.glue.errors.CallSite
import com.amazonaws.services.glue.util.GlueArgParser
import com.amazonaws.services.glue.util.Job
import com.amazonaws.services.glue.util.JsonOptions
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{ StringType, StructType, StructField }
import scala.collection.JavaConverters._

case class Employee(id: String, firstname: String, lastname: String,email: String, phone: String, salary: String, designation: String, manager: String,manageremail: String, department: String)
case class EmployeeTimeLog(id: String, date: String, in: String, out: String)
case class TimeComplaintEmployee(id: String, firstname: String, lastname: String, email: String, phone: String, salary: String, designation: String, manager: String,manageremail: String, department: String, date: String, isTailgated: Integer, isAbsent: Integer)

object GlueApp {
  def main(sysArgs: Array[String]) {
    val sc: SparkContext = new SparkContext()
    val glueContext: GlueContext = new GlueContext(sc)
    val spark = glueContext.getSparkSession
    import spark.implicits._
    
    val args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME", "SPARK_JOB_INPUT_S3_BUCKET_URI", "SPARK_JOB_OUTPUT_S3_BUCKET_URI", "GLUE_DATABASE_NAME", "GLUE_SOURCE_TABLE_NAME", "GLUE_DESTINATION_TABLE_NAME").toArray)
    Job.init(args("JOB_NAME"), glueContext, args.asJava)
    
    val inputPath = args("SPARK_JOB_INPUT_S3_BUCKET_URI") 
    val outputPath = args("SPARK_JOB_OUTPUT_S3_BUCKET_URI")
    val glueDb = args("GLUE_DATABASE_NAME")
    val glueSrcTbl = args("GLUE_SOURCE_TABLE_NAME")
    val glueDestTbl = args("GLUE_DESTINATION_TABLE_NAME")
    
    val datasource0 = glueContext.getCatalogSource(database = glueDb, tableName = glueSrcTbl, redshiftTmpDir = "", transformationContext = "datasource0").getDynamicFrame().toDF(Seq.empty[ResolveSpec]).as[Employee].collect
    val datasource1 = spark.read.schema(StructType(StructField("id", StringType, false) :: StructField("date", StringType, false) :: StructField("in", StringType, true) :: StructField("out", StringType, true) :: Nil)).csv(inputPath).as[EmployeeTimeLog]
    
    val absentEmployee = datasource1.filter(etl => etl.in == null && etl.out == null)
    val tailgatedEmployee = datasource1.filter(etl => etl.in != null && etl.out == null)
    
    val tcabse = absentEmployee.map(absemp => {
        val e = datasource0.filter(_.id==absemp.id)(0)
        TimeComplaintEmployee(e.id, e.firstname, e.lastname, e.email, e.phone, e.salary, e.designation, e.manager, e.manageremail, e.department, absemp.date, 0, 1)
    })
    val tctge = tailgatedEmployee.map(tgemp => {
        val e = datasource0.filter(_.id==tgemp.id)(0)
        TimeComplaintEmployee(e.id, e.firstname, e.lastname, e.email, e.phone, e.salary, e.designation, e.manager, e.manageremail, e.department, tgemp.date, 1, 0)
    })
    val timeComplaintEmpDS = tcabse.union(tctge).sort($"id".asc)
    timeComplaintEmpDS.coalesce(1).write.option("header", "false").csv(outputPath)
    
    val transformed_datasource0 = DynamicFrame(timeComplaintEmpDS.toDF, glueContext);
    
    val datasink5 = glueContext.getCatalogSink(database = glueDb, tableName = glueDestTbl, redshiftTmpDir = "", transformationContext = "datasink5").writeDynamicFrame(transformed_datasource0)
    Job.commit()
  }
}
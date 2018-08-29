package com.etl.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ StringType, StructType, StructField }

case class Employee(id: String, firstname: String, lastname: String,email: String, phone: String, salary: String, designation: String, manager: String,manageremail: String, department: String)
case class EmployeeTimeLog(id: String, date: String, in: String, out: String)
case class TimeComplaintEmployee(id: String, firstname: String, lastname: String,
                                 email: String, phone: String, salary: String, designation: String, manager: String,
                                 manageremail: String, department: String, date: String, isTailgated: Integer, isAbsent: Integer)

//Spark job to transform the given input records and store the result into the output path
object Demo {
  
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("Spark app to find time complaint employees").getOrCreate()
    import spark.implicits._ // For implicit conversions like converting RDDs to DataFrames

    val inputFilePath = args(0)
    val outputFilePath = args(1)
    val jdbcUrl = args(2)
    val dbtable = args(3)
    val dbuser = args(4)
    val dbpassword = args(5)
    val dbdriver = args(6)
    
    //TODO Broadcast this variable
    val employeeList = spark.read
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", dbtable)
      .option("user", dbuser)
      .option("password", dbpassword)
      .option("driver", dbdriver)
      .load()
      .as[Employee]
      .collect()
    val employeeTimeLogDataset = spark.read.schema(StructType(StructField("id", StringType, false) :: StructField("date", StringType, false) :: StructField("in", StringType, true) :: StructField("out", StringType, true) :: Nil)).csv(inputFilePath).as[EmployeeTimeLog] //"/home/ec2-user/suresh/input-emp_time_entries.txt"
    val absentEmployee = employeeTimeLogDataset.filter(etl => etl.in == null && etl.out == null)
    val tailgatedEmployee = employeeTimeLogDataset.filter(etl => etl.in != null && etl.out == null)
    val tcabse = absentEmployee.map(absemp => {
        val e = employeeList.filter(_.id==absemp.id)(0)
        TimeComplaintEmployee(e.id, e.firstname, e.lastname, e.email, e.phone, e.salary, e.designation, e.manager, e.manageremail, e.department, absemp.date, 0, 1)
    })
    val tctge = tailgatedEmployee.map(tgemp => {
        val e = employeeList.filter(_.id==tgemp.id)(0)
        TimeComplaintEmployee(e.id, e.firstname, e.lastname, e.email, e.phone, e.salary, e.designation, e.manager, e.manageremail, e.department, tgemp.date, 1, 0)
    })
    val timeComplaintEmpDS = tcabse.union(tctge).sort($"id".asc)
    timeComplaintEmpDS.coalesce(1).write.option("header", "false").csv(outputFilePath)
    spark.close
  }
}

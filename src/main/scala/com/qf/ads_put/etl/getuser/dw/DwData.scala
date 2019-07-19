package com.qf.ads_put.etl.getuser.dw

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}


object DwData {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf()
      .set("hive.exec.dynamic.partition", "true")
      .set("hive.exec.dynamic.partition.mode", "nonstrict")
      .set("spark.sql.shuffle.partitions", "32")
      .set("hive.merge.mapfiles", "true")
      .set("hive.input.format", "org.apache.hadoop.hive.ql.io.CombineHiveInputFormat")
      .set("spark.sql.autoBroadcastJoinThreshold", "50485760")
      .set("spark.sql.crossJoin.enabled", "true")
      //.set("spark.sql.warehouse.dir","hdfs://hdfsCluster/sparksql/db")
      .setAppName("getuser").setMaster("local[4]")

    val session: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    val sc: SparkContext = session.sparkContext

//    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

    val frame: DataFrame = session.read.table("code_add_ods_1.ods_log")

//   宽表
      val total=frame.selectExpr(DwColumns.getColumns:_*)
    import session.implicits._
//    - 每天获取到的目标客户量
    val total_1 = total.where($"release_status" === "1")
        total_1.count
//    - 不同渠道的目标客户量
    total_1.where($"release_status" === "1").groupBy("channels").count()
//    - 不同区域的目标客户量
    total_1.where($"area_code">40 ).count()

//    - 不同时间段的目标客户量
    total_1.where($"ct_String">6 ||$"ct_String"<=12).count()
    total_1.where($"ct_String">12 ||$"ct_String"<18).count()
    total_1.where($"ct_String">18 ||$"ct_String"<6).count()
//    - 不同年龄段用户的目标客户量
    total.where($"age">40 ).count()
    total.where($"age"<=40).count()




























//    frame.show(10)
//      frame.select("sid","device_num")
////    StructType(StructField(sid,StringType,true), StructField(device_num,StringType,true))
//      val schema = frame.schema

//    import hiveContext.implicits._
//    hiveContext.sql("use add_dim")


//    hiveContext.sql("CREATE TABLE stu12 as SELECT * FROM stu1")
//    hiveContext.sql("SELECT * FROM stu12").show
    import session.implicits._

  }

}

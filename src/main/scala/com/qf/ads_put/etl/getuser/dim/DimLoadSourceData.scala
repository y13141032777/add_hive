package com.qf.ads_put.etl.getuser.dim

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}


object DimLoadSourceData {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf()

      //.set("spark.sql.warehouse.dir","hdfs://hdfsCluster/sparksql/db")
      .setAppName("getuser").setMaster("local[4]")

    val session: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    val context: SparkContext = session.sparkContext

    val frame: DataFrame = session.read.parquet("D:/a.txt")
//    frame.show(10)
//      frame.select("sid","device_num")
//    StructType(StructField(sid,StringType,true), StructField(device_num,StringType,true))
//      val schema = frame.schema
//     import session.implicits._
     val ds = frame.selectExpr(DimColumns.getColumns:_*)
      session.sql("create database if not exists code_add_ods_1")
      ds.write.saveAsTable("code_add_ods_1.ods_log")


  }


}

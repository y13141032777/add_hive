package com.qf.ads_put.etl.getuser.ods

import java.util
import java.util._

import com.alibaba.fastjson.JSON
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}



object OdsLoadSourceData {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf()

      //.set("spark.sql.warehouse.dir","hdfs://hdfsCluster/sparksql/db")
      .setAppName("getuser").setMaster("local[1]")

    val session: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

//    val context: SparkContext = session.sparkContext
//
    val frame: DataFrame = session.read.parquet("D:/a/dianshanglog")
    frame.show

//    frame.show(10)
////      frame.select("sid","device_num")
////    StructType(StructField(sid,StringType,true), StructField(device_num,StringType,true))
////      val schema = frame.schema
////     import session.implicits._
//     val ds = frame.selectExpr(OdsColumns.getColumns:_*)

//      session.sql("create database if not exists code_add_ods_1")
//      ds.write.saveAsTable("code_add_ods_1.ods_log")


//      x=>{
//       val map= JSON.parseFull(x.getString(0)).getOrElse() match{
//         case Some(map: Map[String, Any]) => map
//         case Some(map: HashTrieMap[String, Any]) => map
//        }





  }


}

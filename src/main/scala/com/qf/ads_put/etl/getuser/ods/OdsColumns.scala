package com.qf.ads_put.etl.getuser.ods

import scala.collection.mutable.ArrayBuffer


object OdsColumns {


  def getColumns={
   val columns = ArrayBuffer[String]()
    columns += "sid"
    columns += "device_num"
    columns += "release_session"
    columns += "release_status"
    columns += "device_type"
    columns += "sources"
    columns += "channels"
    columns += "exts"
    columns += "ct"

    columns
  }


}

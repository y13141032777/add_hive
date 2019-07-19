package com.qf.ads_put.etl.getuser.dw

import scala.collection.mutable.ArrayBuffer


object DwColumns {


  def getColumns={
   val columns = ArrayBuffer[String]()
    columns += "sid"
    columns += "device_num"
    columns += "release_session"
    columns += "release_status"
    columns += "device_type"
    columns += "sources"
    columns += "channels"
    columns += "ct"
    columns += "get_json_object(exts,'$area_code') as area_code"
    columns += "floor((unix_timestamp() - unix_timestamp(substring(get_json_object(exts,'$.idcard\'),7,8), 'yyyyMMdd'))/60/60/24/365.25) as age"
    columns += "from_unixtime(cast(ct/1000 as bigint))) as ct_String"

    columns
  }


}

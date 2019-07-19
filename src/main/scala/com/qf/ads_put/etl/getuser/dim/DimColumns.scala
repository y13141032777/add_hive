package com.qf.ads_put.etl.getuser.dim

import scala.collection.mutable.ArrayBuffer


object DimColumns {


  def getColumns={
   val columns = ArrayBuffer[String]()
    columns += "provinceName"
    columns += "provinceNo"
    columns += "cityName"
    columns += "cituNO"
    columns += "townName"
    columns += "townNo"

    columns
  }


}

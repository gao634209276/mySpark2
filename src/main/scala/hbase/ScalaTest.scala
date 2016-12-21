package hbase

/**
  */
object ScalaTest {
	def main(args: Array[String]) {
		val colArray: Array[String] = Array("ID",
			"DATAPROVINCE", "CITYCODE", "NETTYPE", "PAYMENTTYPE", "PUBLISHSTATUS", "RELEASECHANNEL",
			"STARTTIMES", "ENDTIMES", "PRODUCTNAME", "PRODUCTCODE", "PACKAGECODE", "PACKAGENAME",
			"FLOWKINDS", "EFFICWAYS", "TARIFFCODE")
		for (i <- colArray.indices) {
			println(colArray(i))
		}
	}

}

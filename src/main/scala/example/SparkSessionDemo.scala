package example

import java.util.Properties

import org.apache.spark.sql.SparkSession

/**
	* Created by 小小科学家 on 2016/12/8.
	*/
object SparkSessionDemo {


	def main(args: Array[String]) {

		val spark = SparkSession.builder()
			.appName("Spark SQL data sources example")
			.master("local")
			.config("spark.some.config.option", "some-value")
			.getOrCreate()
		runBasicDataSourceExample(spark)

		//runDataSetExample(spark)
		//spark.catalog.listTables.show(false)
	}

	private def runBasicDataSourceExample(spark: SparkSession): Unit = {
		// $example on:generic_load_save_functions$
		val usersDF = spark.read.load("src/main/resources/users.parquet")
		// usersDF.select("name", "favorite_color").write.save("target/namesAndFavColors.parquet")
		// $example off:generic_load_save_functions$
		// $example on:manual_load_options$
		val peopleDF = spark.read.format("json").load("src/main/resources/people.json")
		// peopleDF.select("name", "age").write.format("parquet").save("target/namesAndAges.parquet")
		// $example off:manual_load_options$
		// $example on:direct_sql$
		val sqlDF = spark.sql("SELECT * FROM parquet.`src/main/resources/users.parquet`")
		// $example off:direct_sql$
		sqlDF.show()
	}

	def runDataSetExample(spark: SparkSession) = {
		//create a Dataset using spark.range starting from 5 to 100, with increments of 5
		val numDS = spark.range(5, 100, 5)
		// reverse the order and display first 5 items
		numDS.orderBy().show(5)
		//compute descriptive stats and display them
		/*		numDs.describe().show()
				// create a DataFrame using spark.createDataFrame from a List or Seq
				val langPercentDF = spark.createDataFrame(List(("Scala", 35), ("Python", 30), ("R", 15), ("Java", 20)))
				//rename the columns
				val lpDF = langPercentDF.withColumnRenamed("_1", "language").withColumnRenamed("_2", "percent")
				//order the DataFrame in descending order of percentage
				lpDF.orderBy(desc("percent")).show(false)*/
	}

	private def runJdbcDatasetExample(spark: SparkSession): Unit = {
		// $example on:jdbc_dataset$
		// Note: JDBC loading and saving can be achieved via either the load/save or jdbc methods
		// Loading data from a JDBC source
		val jdbcDF = spark.read
			.format("jdbc")
			.option("url", "jdbc:postgresql:dbserver")
			.option("dbtable", "schema.tablename")
			.option("user", "username")
			.option("password", "password")
			.load()

		val connectionProperties = new Properties()
		connectionProperties.put("user", "username")
		connectionProperties.put("password", "password")
		val jdbcDF2 = spark.read
			.jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties)

		// Saving data to a JDBC source
		jdbcDF.write
			.format("jdbc")
			.option("url", "jdbc:postgresql:dbserver")
			.option("dbtable", "schema.tablename")
			.option("user", "username")
			.option("password", "password")
			.save()

		jdbcDF2.write
			.jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties)
		// $example off:jdbc_dataset$
	}
}

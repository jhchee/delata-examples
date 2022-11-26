package jhchee.delta.examples

import io.delta.tables.DeltaTable
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}

import scala.io.Source

object UserDatasetPlayground extends SparkSessionWrapper {
  val userDeltaTablePath = "/tmp/delta/user"
  val relativePath = "./src/main/resources"

  def main(args: Array[String]): Unit = {
    createUserTable()
    loadData(s"$relativePath/user_dataset_v1.csv")
//    mergeUserTable()
//    val df = spark.read.load(userDeltaTablePath)
//    df.show()
    val userDeltaTable = DeltaTable.forPath(userDeltaTablePath)
    userDeltaTable.history().show(10)
  }

  def createUserTable(): Unit = {
    val sql = readSqlFile("migration/Create_User_Table.sql")
    spark.sql(sql)
  }

  def mergeUserTable(): Unit = {
    val deltaTableUser = DeltaTable.forPath(spark, "/tmp/delta/user")
    val schema = new StructType()
      .add("id", LongType)
      .add("first_name", StringType)
      .add("last_name", StringType)
      .add("salary", IntegerType)
      .add("updated_on", DateType)
    val updates = readCSV(s"$relativePath/user_dataset_v2.csv", schema)
    val dfUpdates = updates.toDF
    deltaTableUser
      .as("user")
      .merge(
        dfUpdates.as("updates"),
        "user.id = updates.id")
      .whenMatched()
      .updateExpr(
        Map(
          "first_name" -> "updates.first_name",
          "last_name" -> "updates.last_name",
          "salary" -> "updates.salary"
        ))
      .whenNotMatched
      .insertExpr(
        Map(
          "id" -> "updates.id",
          "first_name" -> "updates.first_name",
          "last_name" -> "updates.last_name",
          "salary" -> "updates.salary"
        ))
      .execute()
  }

  def readSqlFile(filePath: String): String = {
    val lines = Source.fromResource(filePath).getLines()
    lines.mkString
  }

  def loadData(pathName: String): Unit = {
    val schema = new StructType()
      .add("id", LongType)
      .add("first_name", StringType)
      .add("last_name", StringType)
      .add("salary", IntegerType)
      .add("updated_on", DateType)
    val source = readCSV(pathName, schema)
    write(source)
  }

  def write[T](dataset: Dataset[T], overwrite: Boolean = false): Unit = {
    dataset.write
      .format("delta")
      .mode(if (overwrite) SaveMode.Overwrite else SaveMode.Append)
      .save(userDeltaTablePath)
  }

  def readCSV(pathName: String, schema: StructType): DataFrame = {
    val path = new java.io.File(pathName).getCanonicalPath
    spark.read
      .option("header", "true")
      .option("dateFormat", "yyyy-MM-dd")
      .schema(schema).csv(path)
  }

  def executeSql(query: String): Unit = {
    spark.sql(query).show()
  }
}

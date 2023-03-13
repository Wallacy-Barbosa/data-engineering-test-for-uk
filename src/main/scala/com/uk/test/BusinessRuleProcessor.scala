package com.uk.test

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.Logger

import java.io.{File, FileNotFoundException}
import scala.io.Source
import scala.reflect.io.Directory

class BusinessRuleProcessor(inputFilePath: String, outputFilePath: String)
                           (implicit logger: Logger, sparkSession: SparkSession) {


  def checkExtension(inputFilePath: String): String = {
    val inputFileExtension = inputFilePath.split("[.]").last

    val delimiter =
      inputFileExtension match {
        case "csv" => ";"
        case "tsv" => "\t"
        case _ => throw new RuntimeException(s"Unexpected Error: The file extension (.$inputFileExtension) is not acceptable for the application.")
      }

    delimiter
  }

  private def readFile(inputFilePath: String, delimiter: String): scala.collection.mutable.Map[String, String] = {
    val data = scala.collection.mutable.Map[String, String]()

    try
      Source.fromFile(inputFilePath).getLines()
        .toList
        .tail
        .groupBy(identity)
        .view
        .mapValues(_.length)
        .toMap
        .filter(_._2 % 2 == 1)
        .keys
        .foreach(row => {
          val columns = row.split(delimiter)

          val key = if (columns(0).nonEmpty) columns(0) else "0"
          val value = if (columns(1).nonEmpty) columns(1) else "0"

          data += key -> value
        })
    catch {
      case e: FileNotFoundException => throw new FileNotFoundException(s"Unexpected error: File $inputFilePath was not found \n$e")
      case e: Throwable => throw new RuntimeException(s"Unexpected error: $e")
    }

    data
  }

    logger.warn("Step 3 - Checking input file extension")
    private val delimiter = this.checkExtension(inputFilePath = inputFilePath)

    logger.warn("Step 4 - Get odd occurrences from input file")
    private val data = this.readFile(inputFilePath = inputFilePath, delimiter = delimiter)

    logger.warn("Step 5 - Saving Files")
    import sparkSession.implicits._

    private val outputSchema = List("key", "Value")
    private val df = data.toSeq.toDF(outputSchema: _*)

    df
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("encoding", "UTF-8")
      .option("header", "true")
      .option("delimiter", "\t")
      .csv(s"${outputFilePath}_tmp")

    // treating destination path
    private val outputDirectory = new Directory(new File(s"${outputFilePath}_tmp"))
    outputDirectory.list.foreach(file => {
      if (file.extension == "csv")
        new File(file.toString()).renameTo(new File(outputFilePath))
      else
        file.delete()
    })

    outputDirectory.deleteRecursively()

}

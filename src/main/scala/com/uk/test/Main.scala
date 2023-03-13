package com.uk.test

import org.apache.log4j.BasicConfigurator
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

import scala.util.Try

object Main {

  private def createSparkSession: SparkSession = {

    // Create Spark Conf
    val conf = new SparkConf()
      .setAppName("uk_general_de_test")
      .setMaster("local[*]")

    // Create Spark Session
    val sparkSession = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    // Set log level to warning. It means that spark info log will not appear in the console
    sparkSession.sparkContext.setLogLevel("warn")

    sparkSession
  }

  def checkingArguments(args: Array[String]): Unit = {

    if (Try(args(0).nonEmpty).isFailure || Try(args(1).isEmpty).isFailure) throw new IllegalArgumentException("Unexpected Error: Please check the parameters")
    if (args(0).isEmpty || args(1).isEmpty) throw new IllegalArgumentException("Unexpected Error: Please check the parameters")

  }

  def main(args: Array[String]): Unit = {

    // Uncomment the code to simulate the execution of the process
//     val args = Array(
//      "src/test/resources/input_file_path/file1.csv",
//      "src/test/resources/output_file_path/file1.tsv"
//     )

    // Configure log4j library to log process
    BasicConfigurator.configure()
    implicit val logger: Logger = LoggerFactory.getLogger(this.getClass)
    logger.warn("PROCESS STARTED")

    // Create SparkSession object that will be used at the end of the process to save output file
    implicit val sparkSession: SparkSession = createSparkSession

    logger.warn("Step 1 - Checking application arguments")
    checkingArguments(args = args)

    //  checkArguments(arguments = args)

    logger.warn("Step 2 - Parsing application arguments")
    val inputFilePath = args(0)
    val outputFilePath = args(1)

    // Process business rule
    new BusinessRuleProcessor(
      inputFilePath = inputFilePath,
      outputFilePath = outputFilePath
    )

    // Close the Spark Session
    sparkSession.close()

    logger.warn("PROCESS FINISHED")

  }
}

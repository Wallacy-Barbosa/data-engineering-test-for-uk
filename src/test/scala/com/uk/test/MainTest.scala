package com.uk.test

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.{Logger, LoggerFactory}

import java.io.File
import java.nio.file.{Files, Path}
import scala.reflect.io.Directory

class MainTest extends AnyWordSpec with BeforeAndAfterAll {

  var tempDir: Path = _
  implicit var sparkSession: SparkSession = _
  implicit var logger: Logger = _

  override def beforeAll(): Unit = {

    logger = LoggerFactory.getLogger(this.getClass)
    tempDir = Files.createTempDirectory("uk_general_de_test")
    // Create Spark Conf
    val conf = new SparkConf()
      .setAppName("uk_general_de_test")
      .setMaster("local[*]")

    // Create Spark Session
    sparkSession = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    sparkSession.sparkContext.setLogLevel("warn")

  }

  override def afterAll(): Unit = {

    new Directory(new File(s"${tempDir.toString}")).deleteRecursively()
    sparkSession.close()

  }


  "When running the project successfully, it" should {

    "create the output file" in {

      val args = Array(
        "src/test/resources/input_file_path/file2.tsv",
        s"${tempDir.toString}/output_file_path/file2.tsv"
      )

      // Process Business Rule
      new BusinessRuleProcessor(args(0), args(1))

      assert(new File(args(1)).exists())

    }

    "extract data as expected" in {

      val expectedDataFrame = sparkSession
        .read
        .format("csv")
        .option("encoding", "UTF-8")
        .option("header", "true")
        .option("delimiter", "\t")
        .option("inferSchema", "false")
        .load(path = "src/test/resources/output_file_path/file2.tsv")

      val processedDataFrame = sparkSession
        .read
        .format("csv")
        .option("encoding", "UTF-8")
        .option("header", "true")
        .option("delimiter", "\t")
        .option("inferSchema", "false")
        .load(path = s"${tempDir.toString}/output_file_path/file2.tsv")

      assert(expectedDataFrame.printSchema == processedDataFrame.printSchema)
      assert(expectedDataFrame.except(processedDataFrame).count() == 0)

    }
  }

  "when checking parameters, it" should {

    "allow the flow to continue if it was informed correctly" in {

      val args = Array(
        "some/input_file_path/file.csv",
        "some/output_file_path/file.csv"
      )

      Main.checkingArguments(args = args)

      assume(condition = true)

    }

    "Throw IllegalArgumentException if inputFilePath parameter is missing" in {
      assertThrows[IllegalArgumentException] {
        val args = Array(
          "some/output_file_path/file.csv"
        )

        Main.checkingArguments(args = args)
      }
    }

    "Throw IllegalArgumentException if inputFilePath parameter is null" in {
      assertThrows[IllegalArgumentException] {
        val args = Array(
          null,
          "some/output_file_path/file.csv"
        )

        Main.checkingArguments(args = args)
      }
    }

    "Throw IllegalArgumentException if inputFilePath parameter is empty" in {
      assertThrows[IllegalArgumentException] {
        val args = Array(
          "",
          "some/output_file_path/file.csv"
        )

        Main.checkingArguments(args = args)
      }
    }

    "Throw IllegalArgumentException if outputFilePath parameter is missing" in {
      assertThrows[IllegalArgumentException] {
        val args = Array(
          "some/input_file_path/file.csv"
        )

        Main.checkingArguments(args = args)
      }
    }

    "Throw IllegalArgumentException if outputFilePath parameter is null" in {
      assertThrows[IllegalArgumentException] {
        val args = Array(
          "some/input_file_path/file.csv",
          null
        )

        Main.checkingArguments(args = args)
      }
    }

    "Throw IllegalArgumentException if outputFilePath parameter is empty" in {
      assertThrows[IllegalArgumentException] {
        val args = Array(
          "some/input_file_path/file.csv",
          ""
        )

        Main.checkingArguments(args = args)
      }
    }

  }
}

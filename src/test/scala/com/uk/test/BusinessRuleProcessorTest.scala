package com.uk.test

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.mockito.ArgumentMatchers.any
import org.mockito.{Answers, Mockito}
import org.mockito.internal.creation.MockSettingsImpl
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.{Logger, LoggerFactory}

import java.io.File
import java.nio.file.{Files, Path}
import scala.reflect.io.Directory

class BusinessRuleProcessorTest extends AnyWordSpec with BeforeAndAfterAll {

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

  "when checking extension of input file, it" should {

    "return semicolon delimiter if the extension is csv" in {

      val businessRuleProcessor: BusinessRuleProcessor = Mockito.mock(
        classOf[BusinessRuleProcessor],
        new MockSettingsImpl().defaultAnswer(Answers.RETURNS_DEFAULTS).serializable()
      )
      Mockito.when(businessRuleProcessor.checkExtension(any())).thenCallRealMethod()

      assert(businessRuleProcessor.checkExtension(inputFilePath = "some/input_file_path/file.csv") == ";")

    }

    "return tabular delimiter if the extension is tsv" in {

      val businessRuleProcessor: BusinessRuleProcessor = Mockito.mock(
        classOf[BusinessRuleProcessor],
        new MockSettingsImpl().defaultAnswer(Answers.RETURNS_DEFAULTS).serializable()
      )
      Mockito.when(businessRuleProcessor.checkExtension(any())).thenCallRealMethod()

      assert(businessRuleProcessor.checkExtension(inputFilePath = "some/input_file_path/file.tsv") == "\t")

    }


  }
}

/*
 * Copyright 2018 sadikovi
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.sadikovi.sdeq

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest._

/** abstract general testing class */
abstract class UnitTestSuite extends FunSuite with Matchers with OptionValues with Inside
  with Inspectors with BeforeAndAfterAll with BeforeAndAfter with SparkLocal {

  /** Compare two DataFrame objects */
  protected def checkAnswer(df: DataFrame, expected: DataFrame): Unit = {
    val got = df.collect.map(_.toString).sortWith(_ < _)
    val exp = expected.collect.map(_.toString).sortWith(_ < _)
    assert(got.sameElements(exp), s"Failed to compare DataFrame ${got.mkString("[", ", ", "]")} " +
      s"with expected input ${exp.mkString("[", ", ", "]")}")
  }

  protected def checkAnswer(df: DataFrame, expected: Seq[Row]): Unit = {
    val sc = df.sqlContext.sparkContext
    checkAnswer(df, df.sqlContext.createDataFrame(sc.parallelize(expected), df.schema))
  }
}

/** General Spark base */
private[sdeq] trait SparkBase {
  @transient private var _spark: SparkSession = null

  def createSparkSession(): SparkSession

  /** Start (or create) Spark session */
  def startSparkSession(): Unit = {
    stopSparkSession()
    setLoggingLevel(Level.ERROR)
    _spark = createSparkSession()
  }

  /** Stop Spark session */
  def stopSparkSession(): Unit = {
    if (_spark != null) {
      _spark.stop()
    }
    _spark = null
  }

  /**
   * Set logging level globally for all.
   * Supported log levels:
   *      Level.OFF
   *      Level.ERROR
   *      Level.WARN
   *      Level.INFO
   * @param level logging level
   */
  def setLoggingLevel(level: Level) {
    Logger.getLogger("org").setLevel(level)
    Logger.getLogger("akka").setLevel(level)
    Logger.getRootLogger().setLevel(level)
  }

  /** Returns Spark session */
  def spark: SparkSession = _spark
}

/** Spark context with master "local[4]" */
trait SparkLocal extends SparkBase {
  /** Loading Spark configuration for local mode */
  private def localConf: SparkConf = {
    new SparkConf().
      setMaster("local[4]").
      setAppName("spark-local-test").
      set("spark.driver.memory", "1g").
      set("spark.executor.memory", "1g")
  }

  override def createSparkSession(): SparkSession = {
    SparkSession.builder().config(localConf).getOrCreate()
  }
}

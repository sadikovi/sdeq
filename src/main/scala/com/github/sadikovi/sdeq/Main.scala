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

import scala.collection.mutable
import scala.collection.JavaConverters._

import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Dataset, ForeachWriter, SparkSession}
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.functions.{col, split, trim}
import org.bson.Document
import com.mongodb.{MongoClient, MongoClientURI}
import com.mongodb.client.MongoCollection
import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.WriteConfig

/**
 * Main function to run the application.
 * You need to provide following options (use --conf OPT=VALUE on a command line or
 * spark-defaults.conf).
 * - "spark.sdeq.server.host", model server host, default is "localhost"
 * - "spark.sdeq.server.port", model server port, default is "28080"
 * - "spark.sdeq.mongodb.uri", Mongo URI, default is "mongodb://localhost:27017"
 * - "spark.sdeq.history.files", optional path to the history files/directory
 * - "spark.sdeq.stream.host", host to connect to for streaming data, default is "localhost"
 * - "spark.sdeq.stream.port", port to listen to for streaming data, default is "9999"
 * - "spark.sdeq.train.interval", training interval in seconds, default is "2 min"
 */
object Main extends Logging {
  val DATABASE = "recommendations"
  val CP_COLLECTION = "customer_product"
  val SIM_COLLECTION = "similarity"

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("com.mongodb").setLevel(Level.WARN)

    val spark = SparkSession.builder().appName("Recommendations").getOrCreate()
    import spark.implicits._

    // parse configuration options
    val uri = conf(spark, "spark.sdeq.mongodb.uri", "mongodb://localhost:27017")
    val serverHost = conf(spark, "spark.sdeq.server.host", "localhost")
    val serverPort = conf(spark, "spark.sdeq.server.port", "28080")
    val streamHost = conf(spark, "spark.sdeq.stream.host", "localhost")
    val streamPort = conf(spark, "spark.sdeq.stream.port", "9999")
    val trainingInterval = conf(spark, "spark.sdeq.train.interval", s"${2 * 60}").toInt
    val historyFiles = conf(spark, "spark.sdeq.history.files", "")

    require(trainingInterval >= 1, "Training interval must be greater than 1 second")

    // perform initial loading of the data
    val client = new MongoClient(new MongoClientURI(uri))
    loadHistoryData(spark, client, uri, historyFiles)
    var items = loadSimilarity(spark, client, uri, forceRecompute = false)
    logInfo(s"Loaded ${items.count} records of items")
    val modelResult = ModelResult(items)

    // start streaming job
    // normally I would use kafka source, but it is quite a task to set up kafka properly, so
    // we are using socket source instead. Note that all changes that would be required to use
    // kafka is just replacing format and couple of options, such as bootstrap servers and topic.
    //
    // we expect records to be comma separated in the format:
    // CUSTOMER,PRODUCT
    val streamDF = spark.readStream
      .format("socket")
      .option("host", streamHost)
      .option("port", streamPort)
      .load()

    // parse stream data into views and append them into mongo collection
    val views = streamDF
      .select(col("value").cast("string").as("value"))
      .select(split(col("value"), ",").as("value"))
      .select(
        trim(col("value").getItem(0)),
        trim(col("value").getItem(1))
      )
      .as[(String, String)]
      .map { case (customer, product) => Record(customer, product) }

    val query = appendStreamToMongo(views, uri, DATABASE, CP_COLLECTION)

    // start model server
    ModelServer.start("localhost", 28080, modelResult)

    // start internal loop to refresh similarity matrix periodically
    // note that cluster resources should allow running streaming and batch jobs in terms of
    // resources
    try {
      while (true) {
        Thread.sleep(trainingInterval * 1000L)
        logInfo("Updating similarity matrix")
        items.unpersist()
        items = loadSimilarity(spark, client, uri, forceRecompute = true)
        logInfo(s"Loaded ${items.count} records of items")
        modelResult.update(items)
      }
    } catch {
      case _: Throwable => {
        logInfo("Stopping streaming query...")
        query.stop()
      }
    }
  }

  /**
   * Load data and prepare items dataset.
   * This can be called on a cold startup or after history data has been processed.
   * @param spark Spark session
   * @param client Mongo client
   * @param uri Mongo uri
   * @param historyFiles path to the history files
   * @return updated similarity dataset
   */
  def loadHistoryData(
      spark: SparkSession,
      client: MongoClient,
      uri: String,
      historyFiles: String): Unit = {
    import spark.implicits._

    if (loadHistoryFiles(client)) {
      logWarning(s"Could not locate database $DATABASE or collection $CP_COLLECTION, " +
        s"loading history files from $historyFiles")

      require(historyFiles.nonEmpty, "Expected path to the history files, found empty string")

      val ds = spark.read.option("header", "true").csv(historyFiles).as[Record]
      writeMongo(ds.toDF, uri, DATABASE, CP_COLLECTION)
    } else {
      logInfo(s"Located database $DATABASE and collection $CP_COLLECTION")
    }
  }

  /**
   * Load/reload similarity index.
   * When application starts and similarity index exists, we do not need to recompute it under the
   * assumption that customer data has not changed. If `forceRecompute` is true, we always
   * recompute it.
   * @param spark Spark session
   * @param client Mongo client
   * @param uri Mongo uri
   * @param forceRecompute force recomputation if true
   * @return cached similarity index
   */
  def loadSimilarity(
      spark: SparkSession,
      client: MongoClient,
      uri: String,
      forceRecompute: Boolean): Dataset[Similarity] = {
    import spark.implicits._

    if (forceRecompute || loadSimilarity(client)) {
      if (forceRecompute) {
        logWarning("Forced to recompute similarity index")
      } else {
        logWarning("Could not find similarity collection, recomputing it")
      }
      val ds = loadMongo(spark, uri, DATABASE, CP_COLLECTION).as[Record]
      writeMongo(Model.similarityMatrix(ds).toDF, uri, DATABASE, SIM_COLLECTION)
    }
    // load items
    loadMongo(spark, uri, DATABASE, SIM_COLLECTION).as[Similarity].cache
  }

  // =============================
  // == Miscellaneous functions ==
  // =============================

  /** Shortcut for getting configuration option */
  private def conf(spark: SparkSession, key: String, default: String): String = {
    spark.conf.getOption(key).getOrElse(default)
  }

  /** Check if we need to load history files */
  private def loadHistoryFiles(client: MongoClient): Boolean = {
    !collectionExists(client, DATABASE, CP_COLLECTION)
  }

  /** Check if we need to load similarity matrix */
  private def loadSimilarity(client: MongoClient): Boolean = {
    !collectionExists(client, DATABASE, SIM_COLLECTION)
  }

  /** Check if mongo database exists */
  private def databaseExists(client: MongoClient, db: String): Boolean = {
    val iter = client.listDatabaseNames().iterator()
    while (iter.hasNext) {
      if (db == iter.next) return true
    }
    false
  }

  /** Check if mongo collection exists */
  private def collectionExists(client: MongoClient, db: String, coll: String): Boolean = {
    val iter = client.getDatabase(db).listCollectionNames().iterator()
    while (iter.hasNext) {
      if (coll == iter.next) return true
    }
    false
  }

  /** Shortcut for loading data from mongo */
  private def loadMongo(spark: SparkSession, uri: String, db: String, coll: String): DataFrame = {
    spark.read
      .format("com.mongodb.spark.sql")
      .option("spark.mongodb.input.uri", uri)
      .option("spark.mongodb.input.database", db)
      .option("collection", coll)
      .load()
  }

  /** Shortcut for writing data into mongo */
  private def writeMongo(df: DataFrame, uri: String, db: String, coll: String): Unit = {
    df.write
      .format("com.mongodb.spark.sql")
      .option("spark.mongodb.output.uri", uri)
      .option("spark.mongodb.output.database", db)
      .option("collection", coll)
      .mode("overwrite")
      .save()
  }

  /** Shortcut for writing data into mongodb from a stream */
  private def appendStreamToMongo(
      ds: Dataset[Record],
      uri: String,
      db: String,
      coll: String): StreamingQuery = {
    ds.writeStream
      .outputMode("append")
      .foreach(new ForeachWriter[Record] {
        val writeConfig: WriteConfig = WriteConfig(Map("uri" -> s"$uri/$db.$coll"))
        var connector: MongoConnector = _
        var batch: mutable.ArrayBuffer[Document] = _

        override def process(value: Record): Unit = {
          batch.append(value.toDocument())
        }

        override def close(errorOrNull: Throwable): Unit = {
          if (batch.nonEmpty) {
            connector.withCollectionDo(writeConfig, {
              collection: MongoCollection[Document] =>
                collection.insertMany(batch.asJava)
              }
            )
          }
        }

        override def open(partitionId: Long, version: Long): Boolean = {
          connector = MongoConnector(writeConfig.asOptions)
          batch = new mutable.ArrayBuffer[Document]()
          true
        }
      }).start()
  }
}

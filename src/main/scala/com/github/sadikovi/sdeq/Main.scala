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
import scala.util.Try

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Dataset, ForeachWriter, SparkSession}
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.bson.Document
import com.mongodb.{MongoClient, MongoClientURI}
import com.mongodb.client.MongoCollection
import com.mongodb.spark.MongoConnector
import com.mongodb.spark.config.WriteConfig

/**
 * Main function to run the application.
 * You need to provide following options (use --conf OPT=VALUE on a command line or
 * spark-defaults.conf).
 * - "spark.sdeq.mongodb.uri", Mongo URI, e.g. mongodb://127.0.0.1:27017
 * - "spark.sdeq.history.files", optional path to the history files/directory
 * - "spark.sdeq.kafka.servers", list of kafka servers to connect
 * - "spark.sdeq.kafka.topic", kafka topic to connect
 * - "spark.sdeq.train.interval", training interval in seconds
 */
object Main extends Logging {
  val DATABASE = "recommendations"
  val CP_COLLECTION = "customer_product"
  val SIM_COLLECTION = "similarity"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Recommendations").getOrCreate()
    import spark.implicits._

    // parse configuration options
    val uri = conf(spark, "spark.sdeq.mongodb.uri", "mongodb://127.0.0.1:27017")
    val kafkaServers = conf(spark, "spark.sdeq.kafka.servers", "kafka:9092")
    val kafkaTopic = conf(spark, "spark.sdeq.kafka.topic", "updates")
    val trainingInterval = conf(spark, "spark.sdeq.train.interval", s"${5 * 60}").toInt

    require(trainingInterval >= 1, "Training interval must be greater than 1 second")

    // perform initial loading of the data
    val client = new MongoClient(new MongoClientURI(uri))
    var items = reloadData(spark, client, uri)
    logInfo(s"Loaded ${items.count} records of items")

    // start streaming job
    // we expect records to be comma separated in the format:
    // "CUSTOMER,PRODUCT"
    val kafkaDF = spark.
      readStream.
      format("kafka").
      option("kafka.bootstrap.servers", kafkaServers).
      option("subscribe", kafkaTopic).
      load()

    // Schema that matches our Record class
    val schema = StructType(
      StructField("customer", StringType) ::
      StructField("product", StringType) ::
      Nil
    )

    // parse kafka data into views and append them into mongo collection
    val views = spark.read.
      schema(schema).
      csv(kafkaDF.select(col("value").cast("string")).as[String]).
      as[Record]

    val query = writeMongoStream(views, uri, DATABASE, CP_COLLECTION)

    // start internal loop to refresh similarity matrix periodically
    // note that cluster resources should allow running streaming and batch jobs in terms of
    // resources
    try {
      while (true) {
        Thread.sleep(trainingInterval * 1000L)
        logInfo("Updating similarity matrix")
        items = reloadData(spark, client, uri)
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
   * This can be called on startup or during the application lifetime, it will recompute similarity
   * index.
   * @param spark Spark session
   * @param client Mongo client
   * @return updated similarity dataset
   */
  def reloadData(spark: SparkSession, client: MongoClient, uri: String): Dataset[Similarity] = {
    import spark.implicits._

    val historyFiles = conf(spark, "spark.sdeq.history.files", "")
    if (loadHistoryFiles(client)) {
      logWarning(s"Could not locate database $DATABASE or collection $CP_COLLECTION, " +
        s"loading history files from $historyFiles")

      require(historyFiles.nonEmpty, "Expected path to the history files, found empty string")

      val ds = spark.read.option("header", "true").csv(historyFiles).as[Record]
      writeMongo(ds.toDF, uri, DATABASE, CP_COLLECTION)
    } else {
      logInfo(s"Located database $DATABASE and collection $CP_COLLECTION")
    }

    if (!loadSimilarity(client)) {
      logWarning("Could not find similarity collection, recomputing it")

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
    !databaseExists(client, DATABASE) || !collectionExists(client, DATABASE, CP_COLLECTION)
  }

  /** Check if we need to load similarity matrix */
  private def loadSimilarity(client: MongoClient): Boolean = {
    !databaseExists(client, DATABASE) || !collectionExists(client, DATABASE, SIM_COLLECTION)
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
    spark.read.format("com.mongodb.spark.sql").
      option("spark.mongodb.input.uri", uri).
      option("spark.mongodb.input.database", db).
      option("collection", coll).load()
  }

  /** Shortcut for writing data into mongo */
  private def writeMongo(df: DataFrame, uri: String, db: String, coll: String): Unit = {
    df.write.format("com.mongodb.spark.sql").
      option("spark.mongodb.output.uri", uri).
      option("spark.mongodb.output.database", db).
      option("collection", coll).mode("overwrite").save()
  }

  /** Shortcut for writing data into mongodb from a stream */
  private def writeMongoStream(
      ds: Dataset[Record],
      uri: String,
      db: String,
      coll: String): StreamingQuery = {
    ds.writeStream.
      outputMode("append").
      foreach(new ForeachWriter[Record] {
        val writeConfig: WriteConfig = WriteConfig(Map("uri" -> s"$uri/$db.$coll"))
        var connector: MongoConnector = _
        var batch: mutable.ArrayBuffer[Record] = _

        override def process(value: Record): Unit = {
          batch.append(value)
        }

        override def close(errorOrNull: Throwable): Unit = {
          if (batch.nonEmpty) {
            connector.withCollectionDo(writeConfig, { collection: MongoCollection[Document] =>
              collection.insertMany(batch.map { rec =>
                new Document(rec.customer, rec.product)
              }.asJava)
            })
          }
        }

        override def open(partitionId: Long, version: Long): Boolean = {
          connector = MongoConnector(writeConfig.asOptions)
          batch = new mutable.ArrayBuffer[Record]()
          true
        }
      }).start()
  }
}

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

import scala.util.Try

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.mongodb.{MongoClient, MongoClientURI}

object Main extends Logging {
  val DATABASE = "recommendations"
  val CP_COLLECTION = "customer_product"
  val SIM_COLLECTION = "similarity"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Recommendations").getOrCreate()
    import spark.implicits._

    val uri = spark.conf.get("spark.mongodb.uri")
    val historyFiles = Try(spark.conf.get("spark.history.files")).getOrElse("")

    val client = new MongoClient(new MongoClientURI(uri))

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

    val items = loadMongo(spark, uri, DATABASE, SIM_COLLECTION).as[Similarity].cache
    logInfo(s"Loaded ${items.count} records of items")
  }

  def loadHistoryFiles(client: MongoClient): Boolean = {
    !databaseExists(client, DATABASE) || !collectionExists(client, DATABASE, CP_COLLECTION)
  }

  def loadSimilarity(client: MongoClient): Boolean = {
    !databaseExists(client, DATABASE) || !collectionExists(client, DATABASE, SIM_COLLECTION)
  }

  def databaseExists(client: MongoClient, db: String): Boolean = {
    val iter = client.listDatabaseNames().iterator()
    while (iter.hasNext) {
      if (db == iter.next) return true
    }
    false
  }

  def collectionExists(client: MongoClient, db: String, coll: String): Boolean = {
    val iter = client.getDatabase(db).listCollectionNames().iterator()
    while (iter.hasNext) {
      if (coll == iter.next) return true
    }
    false
  }

  def loadMongo(spark: SparkSession, uri: String, db: String, coll: String): DataFrame = {
    spark.read.format("com.mongodb.spark.sql").
      option("spark.mongodb.input.uri", uri).
      option("spark.mongodb.input.database", db).
      option("collection", coll).load()
  }

  def writeMongo(df: DataFrame, uri: String, db: String, coll: String): Unit = {
    df.write.format("com.mongodb.spark.sql").
      option("spark.mongodb.output.uri", uri).
      option("spark.mongodb.output.database", db).
      option("collection", coll).mode("overwrite").save()
  }
}

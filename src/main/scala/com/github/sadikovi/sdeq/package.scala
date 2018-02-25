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

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types._
import org.bson.Document

/**
 * Input record type, we are only interested in customer and product pairs.
 */
case class Record(customer: String, product: String) {
  def toDocument(): Document = {
    new Document().
      append("customer", customer).
      append("product", product)
  }
}

object Record {
  def schema: StructType = {
    StructType(
      StructField("customer", StringType) ::
      StructField("product", StringType) ::
      Nil
    )
  }
}

/**
 * Output type for the similarity matrix.
 * Each record represents i-j pair with the similarity value.
 */
case class Similarity(p1: String, p2: String, value: Double) {
  def toDocument(): Document = {
    new Document().
      append("p1", p1).
      append("p2", p2).
      append("value", value)
  }
}

object Similarity {
  def schema: StructType = {
    StructType(
      StructField("p1", StringType) ::
      StructField("p2", StringType) ::
      StructField("value", DoubleType) ::
      Nil
    )
  }
}

/** Container to keep track of current items and provide functionality to predict */
case class ModelResult(@transient var items: Dataset[Similarity]) {
  /**
   * Update current items.
   * @param res new items
   */
  def update(res: Dataset[Similarity]): Unit = synchronized {
    items = res
  }

  /**
   * Predict top 3 results using current items.
   * @param item current item
   * @return list of predictions with scores
   */
  def predict(item: String): Array[(String, Double)] = {
    Model.predict(items, item, 3)
  }
}

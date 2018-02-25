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

import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * Model to build matrix of cosine similarities for products and make predictions.
 */
private[sdeq] object Model {

  /**
   * Assign zero-based index `idx` for column `inp` of the input Dataset.
   * @param ds input dataset of records, e.g. customers or products
   * @param inp input column to index
   * @param idx index column name to return
   * @return Dataset that contains unique records from the input with a corresponding index
   */
  def index(ds: Dataset[String], inp: String, idx: String): Dataset[(String, Long)] = {
    import ds.sparkSession.implicits._
    // Note that window functions could potentially be computationally expensive as number of
    // customers grows, so I changed "dense_rank().over(Window.orderBy(inp)) - 1)" to use
    // `zipWithIndex` - this is slower, but scales better.
    ds.distinct.rdd.zipWithIndex.toDF(inp, idx).as[(String, Long)]
  }

  /**
   * Compute n x n cosine similarities matrix from the input dataset.
   * @param input Dataset of records "customer-product"
   * @param threshold tuning parameter, trade-off between cost and estimate quality, [0.0, 1.0]
   * @return similarity matrix mapped to product names
   */
  def similarityMatrix(input: Dataset[Record], threshold: Double = 0): Dataset[Similarity] = {
    import input.sparkSession.implicits._

    // Make sure that we get unique "customer" - "viewed product" pairs
    // This could be avoided, if we know that input dataset does not contain duplicates
    val ds = input.distinct

    // unique list of customers with their indices
    val customers = index(ds.select("customer").as[String], "customer", "i")
    // unique list of products with their indices
    val products = index(ds.select("product").as[String], "product", "j").cache

    val data = ds
      .join(customers, "customer")
      .join(products, "product")
      .select(col("i"), col("j"), lit(1.0).as("value")).as[MatrixEntry]

    // compute n x n sparse upper-triangular matrix of cosine similarities between columns of
    // input matrix, n is the number of products
    val matrix = new CoordinateMatrix(data.rdd).toIndexedRowMatrix
    val similarities = matrix.columnSimilarities().entries.toDS

    // reconstruct products, so we can reference by product name, instead of index
    similarities
      .join(products, similarities("i") === products("j"))
      .select(col("product").as("p1"), similarities("j"), col("value"))
      .join(products, "j")
      .select(col("p1"), col("product").as("p2"), col("value")).as[Similarity]
  }

  /**
   * Predict similar items sorted in descending order by their corresponding score.
   * Make sure that items dataset is cached!
   * @param items similarity matrix
   * @param product product name to look up
   * @param top how many top predictions to return, default is 3
   * @return sorted list of "product name - score" pairs
   */
  def predict(
      items: Dataset[Similarity],
      product: String,
      top: Int = 3): Array[(String, Double)] = {
    import items.sparkSession.implicits._

    val res = items
      .filter(col("p1") === product || col("p2") === product)
      .select(when(col("p1") === product, col("p2")).otherwise(col("p1")).as("item"), col("value"))
      .sort(col("value").desc)
      .limit(top)
      .as[(String, Double)]
      .collect

    // Note that if we ask for a product that does not exist, we just return the first items with
    // score of "0". This is okay for the toy example, but should be considered when building an
    // actual app.
    if (top > 0 && res.length == 0) {
      items
        .select("p1", "value")
        .distinct
        .limit(top)
        .as[(String, Double)]
        .collect
        .map { case (product, value) => (product, 0.0) }
    } else {
      res
    }
  }
}

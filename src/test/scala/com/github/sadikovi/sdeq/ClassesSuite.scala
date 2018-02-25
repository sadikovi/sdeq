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

class ClassesSuite extends UnitTestSuite {
  override def beforeAll() {
    startSparkSession()
  }

  override def afterAll() {
    stopSparkSession()
  }

  test("Record, convert") {
    val rec = Record("A", "B")
    val doc = rec.toDocument
    doc.getString("customer") should be ("A")
    doc.getString("product") should be ("B")
  }

  test("Record, schema") {
    val implicits = spark.implicits
    import implicits._

    val ds = Seq(
      Record("A", "B")
    ).toDS

    // compare dtypes instead of schema to bypass nullability
    ds.dtypes should be (Record.schema.map { field => (field.name, field.dataType.toString) })
  }

  test("Similarity, convert") {
    val sim = Similarity("A", "B", 0.1)
    val doc = sim.toDocument
    doc.getString("p1") should be ("A")
    doc.getString("p2") should be ("B")
    doc.getDouble("value") should be (0.1)
  }

  test("Similarity, schema") {
    val implicits = spark.implicits
    import implicits._

    val ds = Seq(
      Similarity("A", "B", 0.1)
    ).toDS

    // compare dtypes instead of schema to bypass nullability
    ds.dtypes should be (Similarity.schema.map { field => (field.name, field.dataType.toString) })
  }

  test("ModelResult, update") {
    val implicits = spark.implicits
    import implicits._

    val ds = Seq(
      Similarity("A", "B", 0.1)
    ).toDS

    val mr = ModelResult(null)
    mr.items should be (null)
    mr.update(ds)
    mr.items should be (ds)
  }

  test("ModelResult, predict") {
    val implicits = spark.implicits
    import implicits._

    val ds = Seq(
      Similarity("A", "B", 0.1)
    ).toDS

    val mr = ModelResult(ds)
    mr.predict("A") should be (Array(("B", 0.1)))
  }
}

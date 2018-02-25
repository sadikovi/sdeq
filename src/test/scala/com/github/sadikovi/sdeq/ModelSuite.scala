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

class ModelSuite extends UnitTestSuite {
  override def beforeAll() {
    startSparkSession()
  }

  override def afterAll() {
    stopSparkSession()
  }

  test("index dataset - unique dataset") {
    val implicits = spark.implicits
    import implicits._

    val ds = Seq("a", "b", "c", "d").toDS
    val res = Model.index(ds, "value", "index").collect
    res.map(_._1).sortWith(_ < _) should be (Array("a", "b", "c", "d"))
    res.map(_._2).sortWith(_ < _) should be (Array(0, 1, 2, 3))
  }

  test("index dataset - dataset with duplicates") {
    val implicits = spark.implicits
    import implicits._

    val ds = Seq("a", "b", "c", "d", "a", "b", "c").toDS
    val res = Model.index(ds, "value", "index").collect
    res.map(_._1).sortWith(_ < _) should be (Array("a", "b", "c", "d"))
    res.map(_._2).sortWith(_ < _) should be (Array(0, 1, 2, 3))
  }

  test("similarity matrix") {
    val implicits = spark.implicits
    import implicits._

    val input = Seq(
      Record("C1", "P101"),
      Record("C1", "P121"),
      Record("C1", "P131"),
      Record("C1", "P201"),
      Record("C2", "P101"),
      Record("C2", "P201"),
      Record("C3", "P121"),
      Record("C3", "P201")
    ).toDS

    val res = Model.similarityMatrix(input).collect.map(_.value).sortWith(_ < _)
    val exp = Seq(0.577, 0.5, 0.707, 0.816, 0.816, 0.707).sortWith(_ < _)

    res.zip(exp).foreach { case (v1, v2) =>
      v1 should be (v2 +- 1e-3)
    }
  }

  test("predict") {
    val implicits = spark.implicits
    import implicits._

    val items = Seq(
      Similarity("P131", "P201", 0.577),
      Similarity("P101", "P121", 0.500),
      Similarity("P121", "P131", 0.707),
      Similarity("P101", "P201", 0.816),
      Similarity("P121", "P201", 0.816),
      Similarity("P101", "P131", 0.707)
    ).toDS

    var res = Model.predict(items, "P101", 3)
    res should be (Array(("P201", 0.816), ("P131", 0.707), ("P121", 0.500)))

    res = Model.predict(items, "P121", 2)
    res should be (Array(("P201", 0.816), ("P131", 0.707)))

    res = Model.predict(items, "P121", 0)
    res should be (Array.empty)
  }

  test("predict for a new product") {
    val implicits = spark.implicits
    import implicits._

    val items = Seq(
      Similarity("P131", "P201", 0.577),
      Similarity("P101", "P121", 0.500),
      Similarity("P121", "P131", 0.707),
      Similarity("P121", "P234", 0.123)
    ).toDS

    var res = Model.predict(items, "P999", 3)
    res.length should be (3)
    res.toSet should be (Set(("P131", 0.0), ("P101", 0.0), ("P121", 0.0)))
  }
}

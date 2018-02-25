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

class ModelServerSuite extends UnitTestSuite {
  override def beforeAll() {
    startSparkSession()
  }

  override def afterAll() {
    stopSparkSession()
  }

  test("parse query") {
    val handler = PredictHandler(null)
    handler.parseQuery("") should be (Map.empty)
    handler.parseQuery("a=1&b=2&c=3") should be (Map("a" -> "1", "b" -> "2", "c" -> "3"))
    handler.parseQuery("a=1&a=2") should be (Map("a" -> "2"))
    handler.parseQuery("a&b&c&d") should be (Map())
  }

  test("process request - invalid") {
    val implicits = spark.implicits
    import implicits._

    val ds = Seq(
      Similarity("A", "B", 0.89)
    ).toDS

    val handler = PredictHandler(ModelResult(ds))
    val (code1, response1) = handler.processRequest("POST", "")
    code1 should be (400)
    response1 should be ("Error: Invalid request method")

    val (code2, response2) = handler.processRequest("GET", "")
    code2 should be (400)
    response2 should be ("Error: Customer is not provided")

    val (code3, response3) = handler.processRequest("GET", "customer=C")
    code3 should be (400)
    response3 should be ("Error: Product is not provided")
  }

  test("process request - valid") {
    val implicits = spark.implicits
    import implicits._

    val ds = Seq(
      Similarity("A", "B", 0.89)
    ).toDS

    val handler = PredictHandler(ModelResult(ds))
    var (code, response) = handler.processRequest("GET", "customer=C&product=A")
    code should be (200)
    response should be ("Customer C viewed A, so might also like: B (0.89)")
  }
}

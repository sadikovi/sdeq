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

import java.net.InetSocketAddress

import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import com.sun.net.httpserver._

/**
 * Very basic model server to test functionality of the application.
 * Accepts "GET" requests, e.g. "localhost:28080/predict?customer=A&product=B"
 */
object ModelServer extends Logging {
  /**
   * Start web server.
   * @param host to bind
   * @param port to bind
   * @param model model to use for predictions
   */
  def start(host: String, port: Int, model: ModelResult): Unit = {
    val server = HttpServer.create(new InetSocketAddress(host, port), 0)
    server.createContext("/predict", PredictHandler(model))
    server.setExecutor(null)
    server.start()
    logInfo(s"Started model server on $host:$port")
  }
}

case class PredictHandler(@transient val model: ModelResult) extends HttpHandler {
  /**
   * Simple method to return response as text, not a json.
   * Could easily be extended to return actual JSON, or serve a webpage.
   */
  override def handle(t: HttpExchange): Unit = {
    val (code, response) = processRequest(t.getRequestMethod(), t.getRequestURI().getQuery())
    t.sendResponseHeaders(code, response.length())
    val os = t.getResponseBody()
    os.write(response.getBytes())
    os.close()
  }

  /**
   * Method to process request and return code and response.
   * @param method request method
   * @param query query to parse
   * @return code and response text
   */
  private[sdeq] def processRequest(method: String, query: String): (Int, String) = {
    try {
      if (method != "GET") {
        sys.error("Invalid request method")
      }

      val params = parseQuery(query)
      val customer = params.getOrElse("customer", sys.error("Customer is not provided"))
      val product = params.getOrElse("product", sys.error("Product is not provided"))

      val predictions = model.predict(product).map { case (prod, score) =>
        s"$prod ($score)"
      }.mkString(", ")

      val response = s"Customer $customer viewed $product, so might also like: $predictions"
      (200, response)
    } catch {
      case NonFatal(err) => (400, s"Error: ${err.getMessage}")
    }
  }

  /**
   * Basic uri query parsing.
   * @param query query to parse, e.g. "a=1&b=2&c=3"
   * @return map of query key-value pairs
   */
  private[sdeq] def parseQuery(query: String): Map[String, String] = {
    query.split("&")
      .map { param => param.split("=", 2) }
      .filter { _.length == 2 }
      .map { arr => (arr(0), arr(1)) }
      .toMap
  }
}

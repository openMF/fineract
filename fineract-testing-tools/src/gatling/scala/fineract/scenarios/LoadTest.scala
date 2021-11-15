/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package fineract.scenarios

import fineract.{Client, Loan}
import io.gatling.core.Predef._
import io.gatling.http.Predef._

import scala.util.Random

class LoadTest extends Simulation {

  val httpProtocol = http
    .baseUrl("http://localhost:8080/users") // Here is the root for all relative URLs
    .inferHtmlResources()
    .acceptHeader("text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
    .acceptEncodingHeader("gzip, deflate, br")
    //.header("Fineract-Platform-TenantId", "default")
    //.authorizationHeader("Basic bWlmb3M6cGFzc3dvcmQ=")
    .upgradeInsecureRequestsHeader("1")
    .contentTypeHeader("application/json")

  //Global params
  var username = "victor"
  var password = "password"

  //Processes
  var user = new User()
  //var loan = new Loan()


  val scn = scenario("Create user")
    .repeat(1) {
      exec(_.set("username", username).set("password", password))
        .exec(user.create)
    }

  setUp(scn.inject(
    constantConcurrentUsers(10) during (60)
  ).protocols(httpProtocol))
}

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

import java.util.UUID
import scala.util.Random

class ReadTest extends Simulation {

  val randomNumbers = Iterator.continually(
    Map("additionalDisbursementChance" -> (Random.nextInt(4) + 1),
      "loanNoRandomNumber" -> (Random.nextInt(16) + 5),
      "uuid" -> UUID.randomUUID().toString(),
      "loanId" -> (Random.nextInt(latestLoanId) + 1), // Need to amend based on the available loans
      "clientId" -> (Random.nextInt(latestClientId) + 1)
    )
  )


  val httpProtocol = http
    .baseUrl("https://fineract-read.ps.mifos.io/fineract-provider/api/v1")
    .inferHtmlResources()
    .acceptHeader("text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
    .acceptEncodingHeader("gzip, deflate, br")
    .header("Fineract-Platform-TenantId", "default")
    .authorizationHeader("Basic bWlmb3M6cGFzc3dvcmQ=")
    .upgradeInsecureRequestsHeader("1")
    .contentTypeHeader("application/json")

  //Global params
  var productId = 1
  var date = "20 October 2021"
  var paymentDate = "25 October 2021"
  var latestLoanId = 1000152
  var latestClientId = 100067

  //Processes
  var client = new Client()
  var loan = new Loan()


  val scn = scenario("Read loan details")
    .feed(randomNumbers)
    .exec(_.set("date", date).set("productId", productId).set("paymentDate", paymentDate))
    .exec(loan.getDetails)

  val scn2 = scenario("Read loan transactions")
    .repeat(50) {
      feed(randomNumbers).exec(_.set("date", date).set("productId", productId).set("paymentDate", paymentDate))
        .exec(loan.getTransactions)
    }
  val scn3 = scenario("Read loans of customer")
    .feed(randomNumbers).exec(_.set("date", date).set("productId", productId).set("paymentDate", paymentDate))
    .exec(loan.getLoansOfCustomer)

  val scn4 = scenario("Calculate loan schedules")
    .repeat(100) {
      feed(randomNumbers).exec(_.set("date", date).set("productId", productId).set("paymentDate", paymentDate))
        .exec(loan.calculateLoanSchedule)
    }
  setUp(
    //    scn.inject(
    //      atOnceUsers(20)),
    scn.inject(
      constantConcurrentUsers(300) during(360)),
    //    scn3.inject(
    //      atOnceUsers(20)),
//    scn4.inject(
//      atOnceUsers(100))
    //constantConcurrentUsers(300) during(60)
  ) protocols (httpProtocol)
}

/*
    incrementConcurrentUsers(10) // Int
      .times(35)
      .eachLevelLasting(60.seconds)
      .separatedByRampsLasting(60.seconds)
      .startingFrom(1)
 */
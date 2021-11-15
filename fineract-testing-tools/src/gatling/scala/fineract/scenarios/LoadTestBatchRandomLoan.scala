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

class LoadTestBatchRandomLoan extends Simulation {

  val randomNumbers = Iterator.continually(
    // Random number will be accessible in session under variable "OrderRef"
    Map("codeValueRandomNumber" -> Random.nextInt(38),
      "additionalDisbursementChance" -> (Random.nextInt(4) + 1),
    )
  )

  def additionalDisbursementChance() = Random.nextInt(4) + 1

  def loanNoRandomNumber() = Random.nextInt(16) + 5

  val httpProtocol = http
    .baseUrl("https://default.ps.mifos.io/fineract-provider/api/v1") // Here is the root for all relative URLs
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

  //Processes
  var client = new Client()
  var loan = new Loan()


  val scn = scenario("Create client, create loan, approve, disburse")
    .repeat(1) {
      feed(randomNumbers)
        .exec(_.set("date", date).set("productId", productId).set("paymentDate", paymentDate))
        .exec(client.createClientInBatch)
        .repeat(loanNoRandomNumber()) {
          exec(loan.createAndApproveInBatch)
            .exec(loan.disburse)
            .exec(loan.autopayInstruction)
        }
    }

  setUp(scn.inject(
    constantConcurrentUsers(1) during (60)
  ).protocols(httpProtocol))
}

/*
    incrementConcurrentUsers(10) // Int
      .times(35)
      .eachLevelLasting(60.seconds)
      .separatedByRampsLasting(60.seconds)
      .startingFrom(1)
 */
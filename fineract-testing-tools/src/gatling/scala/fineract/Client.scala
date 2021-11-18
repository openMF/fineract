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
package fineract

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import io.gatling.core.Predef._
import io.gatling.http.Predef._

import scala.util.Random

class Client {

  private val mapper = new ObjectMapper

  private def parseJson(s: String) = mapper.readValue(s, classOf[JsonNode])

  val create = http("Create client")
    .post("/clients")
    .body(StringBody(
      s"""{
         |    "address": [],
         |    "familyMembers": [],
         |    "officeId": 1,
         |    "legalFormId": 1,
         |    "firstname": "first name",
         |    "lastname": "last name $${incrementalNumber}",
         |    "genderId": 36,
         |    "active": true,
         |    "locale": "en",
         |    "dateFormat": "dd MMMM yyyy",
         |    "activationDate": "$${date}",
         |    "submittedOnDate": "$${date}",
         |    "dateOfBirth": "11 March 1991",
         |    "savingsProductId": null
         |}""".stripMargin))
    .asJson
    .check(jsonPath("$..clientId").exists.saveAs("clientId"))

  val createTaxDetails =
    http("Create client tax details")
      .post(s"/clients/$${clientId}/identifiers")
      .body(StringBody(
        s"""{
              "documentTypeId": 34,
              "status": "Active",
              "documentKey": "$${uuid}"
          }""".stripMargin)
      )
      .asJson


  val createCustomerTagDetails = http("Create client tag details")
    .post(s"/datatables/dt_user_tags/$${clientId}?genericResultSet=true")
    .body(StringBody(
      s"""{
         |    "bankruptcy_tag_cd_bankruptcy_tag": 22,
         |    "pending_fraud_tag_cd_pending_fraud_tag": 24,
         |    "pending_deceased_tag_cd_pending_deceased_tag": 26,
         |    "hardship_tag_cd_hardship_tag": 28,
         |    "active_duty_tag_cd_active_duty_tag": 31,
         |    "locale": "en",
         |    "dateFormat": "dd MMMM yyyy"
         |}""".stripMargin))
    .asJson

  def bankcruptcyRandomNumber() =Random.nextInt(2) + 22
  def fraudRandomNumber() = Random.nextInt(2) + 24
  def deceasedRandomNumber() = Random.nextInt(2) + 26
  def hardshipRandomNumber() = Random.nextInt(2) + 28
  def activeDutyRandomNumber() = Random.nextInt(2) + 30

  val updateCustomerTagDetails =
    http("Update client tag details")
      .put(s"/datatables/dt_user_tags/$${clientId}?genericResultSet=true")
      .body(StringBody(
        s"""{
           |    "bankruptcy_tag_cd_bankruptcy_tag": ${bankcruptcyRandomNumber()},
           |    "pending_fraud_tag_cd_pending_fraud_tag": ${fraudRandomNumber()},
           |    "pending_deceased_tag_cd_pending_deceased_tag": ${deceasedRandomNumber()},
           |    "hardship_tag_cd_hardship_tag": ${hardshipRandomNumber()},
           |    "active_duty_tag_cd_active_duty_tag": ${activeDutyRandomNumber()},
           |    "locale": "en",
           |    "dateFormat": "dd MMMM yyyy"
           |}""".stripMargin))
      .asJson

  val createClientInBatch = http("Batch - create client, tax and tag details")
    .post("/batches?enclosingTransaction=true")
    .body(StringBody(
      s"""[
         |  {
         |    "requestId": 1,
         |    "relativeUrl": "clients",
         |    "method": "POST",
         |    "headers": [
         |      {
         |        "name": "Content-type",
         |        "value": "application/json"
         |      }
         |    ],
         |    "body": "{
         |      \\"address\\": [],
         |      \\"familyMembers\\": [],
         |      \\"officeId\\": 1,
         |      \\"legalFormId\\": 1,
         |      \\"genderId\\": 36,
         |      \\"firstname\\": \\"first name\\",
         |      \\"lastname\\": \\"last name\\",
         |      \\"active\\": true,
         |      \\"locale\\": \\"en\\",
         |      \\"dateFormat\\": \\"dd MMMM yyyy\\",
         |      \\"activationDate\\": \\"$${date}\\",
         |      \\"submittedOnDate\\": \\"$${date}\\",
         |      \\"dateOfBirth\\": \\"11 March 1991\\",
         |      \\"savingsProductId\\": null
         |    }"
         |  },
         |  {
         |    "requestId": 2,
         |    "relativeUrl": "clients/$$.clientId/identifiers",
         |    "method": "POST",
         |    "reference":1,
         |    "headers": [
         |      {
         |        "name": "Content-type",
         |        "value": "application/json"
         |      }
         |    ],
         |    "body": "{
         |              \\"documentTypeId\\": 34,
         |              \\"status\\": \\"Active\\",
         |              \\"documentKey\\": \\"123\\"
         |          }"
         |  },
         |  {
         |    "requestId": 3,
         |    "relativeUrl": "datatables/dt_user_tags/$$.clientId?genericResultSet=true",
         |    "method": "POST",
         |    "reference":1,
         |    "headers": [
         |      {
         |        "name": "Content-type",
         |        "value": "application/json"
         |      }
         |    ],
         |    "body": "{
         |    \\"bankruptcy_tag_cd_bankruptcy_tag\\": 22,
         |    \\"pending_fraud_tag_cd_pending_fraud_tag\\": 24,
         |    \\"pending_deceased_tag_cd_pending_deceased_tag\\": 26,
         |    \\"hardship_tag_cd_hardship_tag\\": 28,
         |    \\"active_duty_tag_cd_active_duty_tag\\": 31,
         |    \\"locale\\": \\"en\\",
         |    \\"dateFormat\\": \\"dd MMMM yyyy\\"
         |}"
         |  }
         |]""".stripMargin)).asJson
    .check(jsonPath("$..body").transform(string => parseJson(string.replaceAll("""\\""", "")).findValue("clientId")).exists.saveAs("clientId"))
}

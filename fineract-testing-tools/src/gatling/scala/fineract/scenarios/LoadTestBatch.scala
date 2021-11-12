package fineract.scenarios

import fineract.{Client, Loan}
import io.gatling.core.Predef._
import io.gatling.http.Predef._

import java.util.UUID
import scala.util.Random

class LoadTestBatch extends Simulation {

  val randomNumbers = Iterator.continually(
    Map("additionalDisbursementChance" -> (Random.nextInt(4) + 1),
      "loanNoRandomNumber" -> (Random.nextInt(16) + 5),
      "uuid" -> UUID.randomUUID().toString(),
      "incrementalNumber" -> (prefixForName+increaseNumber())
    )
  )

  def increaseNumber( ) : Int = {
    startingNumber = startingNumber+1
    startingNumber
  }

  val httpProtocol = http
    .baseUrl("https://fineract-write.ps.mifos.io/fineract-provider/api/v1")
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
  var prefixForName = ""
  var startingNumber = 1

  //Processes
  var client = new Client()
  var loan = new Loan()


  val scn = scenario("Create client, create loan, approve, disburse")
    .repeat(1000) {
      feed(randomNumbers)
        .exec(_.set("date", date).set("productId", productId).set("paymentDate", paymentDate))
        .exec(client.create)
//        .exec(client.createTaxDetails)
//        .exec(client.createCustomerTagDetails)
//        .repeat(10) {
//          exec(loan.createAndApproveInBatch)
//            .exec(loan.disburse)
//            .exec(loan.autopayInstruction)
//            .doIfEquals("${additionalDisbursementChance}",4) {
//              exec(client.updateCustomerTagDetails)
//            }
//        }
    }

  setUp(scn.inject(
    atOnceUsers( 1)
  ).protocols(httpProtocol))
}

/*
    incrementConcurrentUsers(10) // Int
      .times(35)
      .eachLevelLasting(60.seconds)
      .separatedByRampsLasting(60.seconds)
      .startingFrom(1)
 */
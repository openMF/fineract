package fineract.playground

import fineract.{Client, Loan}
import io.gatling.core.Predef._
import io.gatling.http.Predef._

import scala.util.Random

class LoadTest extends Simulation {

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
      exec(_.set("date", date).set("productId", productId).set("paymentDate", paymentDate))
        .exec(client.create)
        .exec(loan.createAndApproveInBatch)
        .exec(loan.disburse)
        .exec(loan.createAutopay)
    }

  setUp(scn.inject(
    constantConcurrentUsers(10) during (60)
  ).protocols(httpProtocol))
}
package fineract.scenarios.write

import fineract.{Client, Configuration, Loan}
import io.gatling.core.Predef._
import io.gatling.http.Predef._

import java.util.UUID
import scala.util.Random

class WriteStep0 extends Simulation {

  val randomNumbers = Iterator.continually(
    Map(
      "loanNoRandomNumber" -> (Random.nextInt(16) + 5),
      "uuid" -> UUID.randomUUID().toString(),
      "incrementalNumber" -> (prefixForName + increaseNumber())
    )
  )

  def increaseNumber(): Int = {
    startingNumber = startingNumber + 1
    startingNumber
  }


  val httpProtocol = http
    .baseUrl(Configuration.WriteUrl)
    .inferHtmlResources()
    .acceptHeader("text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
    .acceptEncodingHeader("gzip, deflate, br")
    .header("Fineract-Platform-TenantId", Configuration.TenantId)
    .authorizationHeader("Basic bWlmb3M6cGFzc3dvcmQ=")
    .upgradeInsecureRequestsHeader("1")
    .contentTypeHeader("application/json")

  //Global params
  var productId = Configuration.ProductId
  var date = Configuration.Date
  var paymentDate = Configuration.PaymentDate
  var prefixForName = ""
  var startingNumber = 1

  //Processes
  var client = new Client()
  var loan = new Loan()


  val scn = scenario("Create client, create loan, approve, disburse")
    .repeat(34) {
      feed(randomNumbers)
        .exec(_.set("date", date).set("productId", productId).set("paymentDate", paymentDate))
        .exec(client.create)
        .repeat(10) {
          exec(loan.createAndApproveInBatch)
            .exec(loan.disburse)
            .exec(loan.createAutopay)
        }
    }

  setUp(scn.inject(
    atOnceUsers(300)
  ).protocols(httpProtocol))
}
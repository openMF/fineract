package fineract.scenarios.write

import fineract.{Client, Configuration, Loan}
import io.gatling.core.Predef._
import io.gatling.http.Predef._

class WriteStep2 extends Simulation {

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

  val finiteFeeder = (for (i <- 10 to 10000500) yield {
    Map("loanId" -> s"$i")
  })

  val scn = scenario("Create client, create loan, approve, disburse")
    .repeat(33334) {
      feed(finiteFeeder)
        .exec(_.set("date", date).set("productId", productId).set("paymentDate", paymentDate))
        .exec(loan.repayment)
    }

  setUp(scn.inject(
    atOnceUsers(300)
  ).protocols(httpProtocol))
}
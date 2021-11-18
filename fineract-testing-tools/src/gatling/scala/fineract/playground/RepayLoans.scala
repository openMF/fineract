package fineract.playground

import fineract.{Client, Loan}
import io.gatling.core.Predef._
import io.gatling.http.Predef._

class RepayLoans extends Simulation {

  def increaseNumber(): Int = {
    startingNumber = startingNumber + 1
    startingNumber
  }

  val httpProtocol = http
    .baseUrl("https://fineract-write.ps.mifos.io/fineract-provider/api/v1")
    .inferHtmlResources()
    .acceptHeader("text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
    .acceptEncodingHeader("gzip, deflate, br")
    .header("Fineract-Platform-TenantId", "default3")
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

  val finiteFeeder = (for (i <- 6997000 to 7000000) yield {
    Map("loanId" -> s"$i")
  })


  val scn = scenario("Repay and close loan")
    .repeat(4) {
      feed(finiteFeeder)
        .exec(_.set("paymentDate", paymentDate))
        .exec(loan.repayment)

    }
  setUp(scn.inject(
    atOnceUsers(290)
  ).protocols(httpProtocol))

}

/*
    incrementConcurrentUsers(10) // Int
      .times(35)
      .eachLevelLasting(60.seconds)
      .separatedByRampsLasting(60.seconds)
      .startingFrom(1)
 */
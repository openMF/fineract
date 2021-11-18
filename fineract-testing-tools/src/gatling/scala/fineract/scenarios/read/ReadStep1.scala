package fineract.scenarios.read

import fineract.{Client, Configuration, Loan}
import io.gatling.core.Predef._
import io.gatling.http.Predef._

import scala.concurrent.duration.DurationInt
import scala.util.Random

class ReadStep1 extends Simulation {

  val randomNumbers = Iterator.continually(
    Map(
      "loanId" -> (Random.nextInt(Configuration.maxLoanId) + 1), // Need to amend based on the available loans
      "clientId" -> (Random.nextInt(Configuration.maxClientId) + 1)
    )
  )

  val httpProtocol = http
    .baseUrl(Configuration.ReadUrl)
    .inferHtmlResources()
    .acceptHeader("text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
    .acceptEncodingHeader("gzip, deflate, br")
    .header("Fineract-Platform-TenantId", Configuration.TenantId)
    .authorizationHeader("Basic bWlmb3M6cGFzc3dvcmQ=")
    .upgradeInsecureRequestsHeader("1")
    .contentTypeHeader("application/json")

  //Processes
  var client = new Client()
  var loan = new Loan()


  val retrieveLoan = scenario("Retrieve Loan")
    .feed(randomNumbers)
    .exec(loan.retrieveLoan)

  val retrieveLoansByCustomer = scenario("Retrieve Loans by Customer")
    .feed(randomNumbers)
    .exec(loan.retrieveLoansByCustomer)

  val retrieveRepaymentSummary = scenario("Retrieve Repayment Summary")
    .feed(randomNumbers)
    .exec(loan.retrieveRepaymentSummary)

  val retrieveTransactionsByLoan = scenario("Retrieve Transactions by Loan")
    .feed(randomNumbers)
    .exec(loan.retrieveTransactionsByLoan)


  setUp(
    retrieveLoan.inject(constantConcurrentUsers(180).during(1.minutes)),
    retrieveLoansByCustomer.inject(constantConcurrentUsers(36).during(1.minutes)),
    retrieveRepaymentSummary.inject(constantConcurrentUsers(79).during(1.minutes)),
    retrieveTransactionsByLoan.inject(constantConcurrentUsers(5).during(1.minutes))
  ).protocols(httpProtocol)
}
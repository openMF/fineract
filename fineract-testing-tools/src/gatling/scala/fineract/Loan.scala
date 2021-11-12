package fineract

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import io.gatling.core.Predef._
import io.gatling.http.Predef._

class Loan {

  private val mapper = new ObjectMapper

  private def parseJson(s: String) = mapper.readValue(s, classOf[JsonNode])

  val disburse = http("Disburse loan application")
    .post(s"/loans/$${loanId}?command=disburse")
    .body(StringBody(
      s"""{
         |  "paymentTypeId": 1,
         |  "transactionAmount": 500,
         |  "actualDisbursementDate": "$${date}",
         |  "locale": "en",
         |  "dateFormat": "dd MMMM yyyy"
         |}""".stripMargin)).asJson

  val autopayInstruction = http("Create autopay instruction")
    .post(s"/datatables/dt_autopay_details/$${loanId}?genericResultSet=true")
    .body(StringBody(
      s"""{
         |    "financial_instrument_cd_financial_instruments": 19,
         |    "date_of_payment": "$${date}",
         |    "locale": "en",
         |    "dateFormat": "dd MMMM yyyy"
         |}""".stripMargin)).asJson

  val addDisbursementDetails = http("Add disbursement details")
    .post(s"/loans/$${loanId}/disbursements/editDisbursements")
    .body(StringBody(
      s"""{
         |  "locale": "en",
         |   "dateFormat": "dd MMMM yyyy",
         |  "expectedDisbursementDate": "$${date}",
         |  "disbursementData": [
         |        {
         |            "principal": 100,
         |            "expectedDisbursementDate": "$${date}"
         |        },
         |        {
         |            "principal": 100,
         |            "expectedDisbursementDate": "$${paymentDate}"
         |        }
         |    ],
         |}""".stripMargin)).asJson

  val getDetails = http("Get loan details")
    .get(s"/loans/$${loanId}")
    .body(StringBody(s"""{}""")).asJson

  val getLoansOfCustomer = http("Get loans of customer")
    .get(s"/clients/$${clientId}/accounts")
    .body(StringBody(s"""{}""")).asJson

  val getTransactions = http("Get loan transactions")
    .get(s"/loans/$${loanId}?associations=transactions")
    .body(StringBody(s"""{}""")).asJson

  val calculateLoanSchedule = http("Calculate loan schedule")
    .post("/loans?command=calculateLoanSchedule")
    .body(StringBody(
      s"""{
         |  "dateFormat": "dd MMMM yyyy",
         |  "locale": "en_GB",
         |  "productId": $${productId},
         |  "principal": "1000.00",
         |  "loanTermFrequency": 3,
         |  "loanTermFrequencyType": 2,
         |  "numberOfRepayments": 3,
         |  "repaymentEvery": 1,
         |  "repaymentFrequencyType": 2,
         |  "interestRatePerPeriod": 2,
         |  "amortizationType": 1,
         |  "interestType": 0,
         |  "interestCalculationPeriodType": 1,
         |  "expectedDisbursementDate": "$${date}",
         |  "submittedOnDate": "$${date}",
         |  "transactionProcessingStrategyId": 2,
         |  "loanType": "individual",
         |  "clientId": $${clientId}
         |}""".stripMargin))
    .asJson

  val createAndApproveInBatch = http("Batch - submit, approve loan application")
    .post("/batches")
    .body(StringBody(
      s"""[
         |  {
         |    "requestId": 1,
         |    "relativeUrl": "loans",
         |    "method": "POST",
         |    "headers": [
         |      {
         |        "name": "Content-type",
         |        "value": "application/json"
         |      }
         |    ],
         |    "body": "{
         |      \\"clientId\\": $${clientId},
         |      \\"productId\\": $${productId},
         |      \\"disbursementData\\": [
         |        {
         |            \\"expectedDisbursementDate\\": \\"$${date}\\",
         |            \\"principal\\": \\"100\\"
         |        },
         |        {
         |            \\"expectedDisbursementDate\\": \\"$${paymentDate}\\",
         |            \\"principal\\": \\"100\\"
         |        }
         |    ],
         |      \\"principal\\": 1000,
         |      \\"loanTermFrequency\\": 30,
         |      \\"loanTermFrequencyType\\": 0,
         |      \\"numberOfRepayments\\": 1,
         |      \\"repaymentEvery\\": 30,
         |      \\"repaymentFrequencyType\\": 0,
         |      \\"interestRatePerPeriod\\": 0,
         |      \\"amortizationType\\": 1,
         |      \\"isEqualAmortization\\": false,
         |      \\"interestType\\": 0,
         |      \\"interestCalculationPeriodType\\": 1,
         |      \\"allowPartialPeriodInterestCalcualtion\\": true,
         |      \\"transactionProcessingStrategyId\\": 1,
         |      \\"maxOutstandingLoanBalance\\": 10000,
         |      \\"rates\\": [],
         |      \\"locale\\": \\"en\\",
         |      \\"dateFormat\\": \\"dd MMMM yyyy\\",
         |      \\"loanType\\": \\"individual\\",
         |      \\"expectedDisbursementDate\\": \\"$${date}\\",
         |      \\"submittedOnDate\\": \\"$${date}\\"
         |    }"
         |  },
         |  {
         |    "requestId": 2,
         |    "relativeUrl": "loans/$$.loanId?command=approve",
         |    "method": "POST",
         |    "reference":1,
         |    "headers": [
         |      {
         |        "name": "Content-type",
         |        "value": "application/json"
         |      }
         |    ],
         |    "body": "{
         |      \\"approvedOnDate\\": \\"$${date}\\",
         |      \\"approvedLoanAmount\\": 1000,
         |      \\"expectedDisbursementDate\\": \\"$${date}\\",
         |      \\"disbursementData\\": [
         |         {
         |            \\"principal\\": 100,
         |            \\"expectedDisbursementDate\\": \\"$${date}\\"
         |        },
         |        {
         |            \\"principal\\": 100,
         |            \\"expectedDisbursementDate\\": \\"$${paymentDate}\\"
         |        }
         |      ],
         |      \\"locale\\": \\"en\\",
         |      \\"dateFormat\\": \\"dd MMMM yyyy\\"
         |    }"
         |  }
         |]""".stripMargin)).asJson
    .check(jsonPath("$..body").transform(string => parseJson(string.replaceAll("""\\""", "")).findValue("loanId")).exists.saveAs("loanId"))

  val repayment = http("Repayment for loan")
    .post(s"/loans/$${loanId}/transactions?command=repayment")
    .body(StringBody(
      s"""{
         |  "paymentTypeId": 2,
         |  "transactionAmount": 200,
         |  "transactionDate": "$${paymentDate}",
         |  "locale": "en",
         |  "dateFormat": "dd MMMM yyyy"
         |}""".stripMargin)).asJson
    .check(jsonPath("$..resourceId").exists.saveAs("transactionId"))

  val reverseRepayment = http("Reverse repayment")
    .post(s"/loans/$${loanId}/transactions/$${transactionId}?command=undo")
    .body(StringBody(
      s"""{
         |  "transactionAmount": 0,
         |  "transactionDate": "$${paymentDate}",
         |  "locale": "en",
         |  "dateFormat": "dd MMMM yyyy"
         |}""".stripMargin)).asJson
    .check(jsonPath("$..resourceId").exists.saveAs("transactionId"))


}

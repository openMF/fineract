@AccountTransfer
Feature: AccountTransfer

  @TestRailId:C80937
  Scenario: Transfer from savings to linked loan then undo it
    When Admin sets the business date to "13 May 2026"
    And Admin creates a client with random data
    And Admin creates a EUR savings product
    And Client creates a new EUR savings account with "01 May 2026" submitted on date
    And Approve EUR savings account on "01 May 2026" date
    And Activate EUR savings account on "01 May 2026" date
    And Client successfully deposits 1000 EUR to the savings account on "01 May 2026" date
    Then Savings Transactions tab has the following data:
      | Transaction date | Transaction Type | Amount | Balance |
      | 01 May 2026      | Deposit          | 1000.0 | 1000.0  |
    When Admin creates a fully customized loan with the following data:
      | LoanProduct                             | submitted on date | with Principal | ANNUAL interest rate % | interest type     | interest calculation period | amortization type  | loanTermFrequency | loanTermFrequencyType | repaymentEvery | repaymentFrequencyType | numberOfRepayments | graceOnPrincipalPayment | graceOnInterestPayment | interest free period | Payment strategy            |
      | LP2_ADV_PYMNT_INTEREST_DAILY_EMI_360_30 | 01 May 2026       | 1000           | 12                     | DECLINING_BALANCE | DAILY                       | EQUAL_INSTALLMENTS | 6                 | MONTHS                | 1              | MONTHS                 | 6                  | 0                       | 0                      | 0                    | ADVANCED_PAYMENT_ALLOCATION |
    And Admin successfully approves the loan on "01 May 2026" with "1000" amount and expected disbursement date on "01 May 2026"
    When Admin successfully disburse the loan on "01 May 2026" with "1000" EUR transaction amount
    When Initiate account transfer from savings to loan on "2 May 2026" for 10
    Then Savings Transactions tab has the following data:
      | Transaction date | Transaction Type | Amount | Balance |
      | 01 May 2026      | Deposit          | 1000.0 | 1000.0  |
      | 02 May 2026      | Withdrawal       | 10.0   | 990.0   |
    Then Loan Transactions tab has the following data:
      | Transaction date | Transaction Type | Amount | Principal | Interest | Fees | Penalties | Loan Balance | Reverted | Replayed |
      | 01 May 2026      | Disbursement     | 1000.0 | 0.0       | 0.0      | 0.0  | 0.0       | 1000.0       | false    | false    |
      | 02 May 2026      | Repayment        | 10.0   | 10.0      | 0.0      | 0.0  | 0.0       | 990.0        | false    | false    |
    When Undo the last account transfer
    Then Savings Transactions tab has the following data:
      | Transaction date | Transaction Type | Amount | Balance | Reverted |
      | 01 May 2026      | Deposit          | 1000.0 | 1000.0  | false    |
      | 02 May 2026      | Withdrawal       | 10.0   | 0.0     | true     |
    Then Loan Transactions tab has the following data:
      | Transaction date | Transaction Type | Amount | Principal | Interest | Fees | Penalties | Loan Balance | Reverted | Replayed |
      | 01 May 2026      | Disbursement     | 1000.0 | 0.0       | 0.0      | 0.0  | 0.0       | 1000.0       | false    | false    |
      | 02 May 2026      | Repayment        | 10.0   | 10.0      | 0.0      | 0.0  | 0.0       | 990.0        | true     | false    |
    When Undo the last account transfer it fails with error: it is already reverted

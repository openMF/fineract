Feature: Datatables

  @TestRailId:C3005
  Scenario: Datatable's primary key is unique and indexed
    When A datatable for "Loan" is created
    Then The following column definitions match:
      | Name    | Primary key | Unique | Indexed |
      | loan_id | true        | true   | true    |

  @TestRailId:C3006
  Scenario: Multirow datatable's primary key is unique and indexed
    When A multirow datatable for "Loan" is created
    Then The following column definitions match:
      | Name | Primary key | Unique | Indexed |
      | id   | true        | true   | true    |

  @TestRailId:C3007
  Scenario: Multirow datatable has the foreign key indexed
    When A multirow datatable for "Loan" is created
    Then The following column definitions match:
      | Name    | Primary key | Unique | Indexed |
      | loan_id | false       | false  | true    |

  @TestRailId:C3008
  Scenario: Datatable with unique constrained column is indexed
    When A datatable for "Loan" is created with the following extra columns:
      | Name | Type   | Length | Unique | Indexed |
      | col1 | string | 10     | true   | false   |

    Then The following column definitions match:
      | Name | Primary key | Unique | Indexed |
      | col1 | false       | true   | true    |

  @TestRailId:C3009
  Scenario: Datatable with indexed column
    When A datatable for "Loan" is created with the following extra columns:
      | Name | Type   | Length | Unique | Indexed |
      | col1 | string | 10     | false  | true    |

    Then The following column definitions match:
      | Name | Primary key | Unique | Indexed |
      | col1 | false       | false  | true    |

  @TestRailId:C3010 @TestRailId:C3011 @TestRailId:C3012
  Scenario Outline: Query data from datatable with invalid filter
    When A datatable for "Loan" is created
    And The client calls the query endpoint for the created datatable with "<column_filter>" column filter, and "<value_filter>" value filter
    Then The status of the HTTP response should be 400
    And The response body should contain the following message: "<error_message>"
    Examples:
      | column_filter | value_filter | error_message                            |
      | loan_id       | InvalidInput | validation.msg.invalid.integer.format    |
      | created_at    | InvalidInput | validation.msg.invalid.dateFormat.format |
      | invalidColumn | InvalidInput | validation.msg.validation.errors.exist   |

  @TestRailId:C85156
  Scenario: Datatable entry create with null value persists column via client JSON body
    When Admin sets the business date to "01 January 2026"
    And Admin creates a client with random data
    And Admin creates a fully customized loan with the following data:
      | LoanProduct  | submitted on date | with Principal | ANNUAL interest rate % | interest type     | interest calculation period | amortization type  | loanTermFrequency | loanTermFrequencyType | repaymentEvery | repaymentFrequencyType | numberOfRepayments | graceOnPrincipalPayment | graceOnInterestPayment | interest free period | Payment strategy                            |
      | LP1_DUE_DATE | 01 January 2026   | 1000           | 0                      | DECLINING_BALANCE | SAME_AS_REPAYMENT_PERIOD    | EQUAL_INSTALLMENTS | 1                 | MONTHS                | 1              | MONTHS                 | 1                  | 0                       | 0                      | 0                    | PENALTIES_FEES_INTEREST_PRINCIPAL_ORDER |
    And A datatable for "Loan" is created with the following extra columns:
      | Name   | Type   | Length | Unique | Indexed |
      | amount | number | 10     | false  | false   |
    And A datatable entry is created for "Loan" with null in column "amount"
    Then Fetching the datatable entry for "Loan" returns null in column "amount"

  @TestRailId:C85157
  Scenario: Datatable entry update with null value clears column via client JSON body
    When Admin sets the business date to "01 January 2026"
    And Admin creates a client with random data
    And Admin creates a fully customized loan with the following data:
      | LoanProduct  | submitted on date | with Principal | ANNUAL interest rate % | interest type     | interest calculation period | amortization type  | loanTermFrequency | loanTermFrequencyType | repaymentEvery | repaymentFrequencyType | numberOfRepayments | graceOnPrincipalPayment | graceOnInterestPayment | interest free period | Payment strategy                            |
      | LP1_DUE_DATE | 01 January 2026   | 1000           | 0                      | DECLINING_BALANCE | SAME_AS_REPAYMENT_PERIOD    | EQUAL_INSTALLMENTS | 1                 | MONTHS                | 1              | MONTHS                 | 1                  | 0                       | 0                      | 0                    | PENALTIES_FEES_INTEREST_PRINCIPAL_ORDER |
    And A datatable for "Loan" is created with the following extra columns:
      | Name   | Type   | Length | Unique | Indexed |
      | amount | number | 10     | false  | false   |
    And A datatable entry is created for "Loan" with value "100" in column "amount"
    Then Fetching the datatable entry for "Loan" returns value "100" in column "amount"
    When The datatable entry for "Loan" is updated with null in column "amount"
    Then Fetching the datatable entry for "Loan" returns null in column "amount"

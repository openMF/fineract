@WorkingCapitalDiscountFeature
Feature: Working Capital Discount

  @TestRailId:C72393
  Scenario: Discount on WCL account added after disbursement as on the same as disburse date processed successfully -  UC1
    When Admin sets the business date to "01 January 2026"
    And Admin creates a client with random data
    And Admin creates a working capital loan with the following data:
      | LoanProduct | submittedOnDate | expectedDisbursementDate | principalAmount | totalPayment | periodPaymentRate | discount |
      | WCLP        | 01 January 2026 | 01 January 2026          | 100             | 100          | 1                 |          |
    Then Working capital loan creation was successful
    And Working capital loan account has the correct data:
      | product.name | submittedOnDate | expectedDisbursementDate | status                         | principal | approvedPrincipal | totalPayment | periodPaymentRate | discount |
      | WCLP         | 2026-01-01      | 2026-01-01               | Submitted and pending approval | 100.0     | 0.0               | 100.0        | 1.0               | null     |
    Then Admin successfully approves the working capital loan on "01 January 2026" with "100" amount and expected disbursement date on "01 January 2026"
    Then Working capital loan approval was successful
    And Working capital loan account has the correct data:
      | product.name | submittedOnDate | expectedDisbursementDate | status   | principal | approvedPrincipal | totalPayment | periodPaymentRate | discount |
      | WCLP         | 2026-01-01      | 2026-01-01               | Approved | 100.0     | 100.0             | 100.0        | 1.0               | null     |
    Then Admin successfully disburse the Working Capital loan on "01 January 2026" with "100" EUR transaction amount
    Then Working Capital loan status will be "ACTIVE"
    Then Verify Working Capital loan disbursement was successful
    And Working capital loan account has the correct data:
      | product.name | submittedOnDate | expectedDisbursementDate | status | principal | approvedPrincipal | totalPayment | periodPaymentRate | discount |
      | WCLP         | 2026-01-01      | 2026-01-01               | Active | 100.0     | 100.0             | 100.0        | 1.0               | null     |
    And Working Capital Loan has transactions:
      | transactionDate | type         | transactionAmount | principalPortion | feeChargesPortion | penaltyChargesPortion | reversed |
      | 01 January 2026 | Disbursement | 100.0             | 100.0            | 0.0               | 0.0                   | false    |
  # --- add discount after disbursement on the same disbursement date --- #
    Then Admin adds Discount fee with "12" amount on Working Capital loan account for last disbursement
    And Working capital loan account has the correct data:
      | product.name | submittedOnDate | expectedDisbursementDate | status | principal | approvedPrincipal | totalPayment | periodPaymentRate | discount |
      | WCLP         | 2026-01-01      | 2026-01-01               | Active | 112.0     | 100.0             | 100.0        | 1.0               | 12.0     |
    And Working Capital Loan has transactions:
      | transactionDate | type         | transactionAmount | principalPortion | feeChargesPortion | penaltyChargesPortion | reversed |
      | 01 January 2026 | Disbursement | 100.0             | 100.0            | 0.0               | 0.0                   | false    |
      | 01 January 2026 | Discount Fee | 12.0              | 12.0             | 0.0               | 0.0                   | false    |

  @TestRailId:C72394
  Scenario: Discount update on WCL account on diff from disbursement date outcomes with an error - UC2
    When Admin sets the business date to "01 January 2026"
    And Admin creates a client with random data
    And Admin creates a working capital loan with the following data:
      | LoanProduct | submittedOnDate | expectedDisbursementDate | principalAmount | totalPayment | periodPaymentRate | discount |
      | WCLP        | 01 January 2026 | 01 January 2026          | 100             | 100          | 1                 |          |
    Then Working capital loan creation was successful
    And Working capital loan account has the correct data:
      | product.name | submittedOnDate | expectedDisbursementDate | status                         | principal | approvedPrincipal | totalPayment | periodPaymentRate | discount |
      | WCLP         | 2026-01-01      | 2026-01-01               | Submitted and pending approval | 100.0     | 0.0               | 100.0        | 1.0               | null     |
    Then Admin successfully approves the working capital loan on "01 January 2026" with "100" amount and expected disbursement date on "01 January 2026"
    Then Working capital loan approval was successful
    And Working capital loan account has the correct data:
      | product.name | submittedOnDate | expectedDisbursementDate | status   | principal | approvedPrincipal | totalPayment | periodPaymentRate | discount |
      | WCLP         | 2026-01-01      | 2026-01-01               | Approved | 100.0     | 100.0             | 100.0        | 1.0               | null    |
    Then Admin successfully disburse the Working Capital loan on "01 January 2026" with "100" EUR transaction amount
    Then Working Capital loan status will be "ACTIVE"
    Then Verify Working Capital loan disbursement was successful
    And Working capital loan account has the correct data:
      | product.name | submittedOnDate | expectedDisbursementDate | status | principal | approvedPrincipal | totalPayment | periodPaymentRate | discount |
      | WCLP         | 2026-01-01      | 2026-01-01               | Active | 100.0     | 100.0             | 100.0        | 1.0               | null     |
    And Working Capital Loan has transactions:
      | transactionDate | type         | transactionAmount | principalPortion | feeChargesPortion | penaltyChargesPortion | reversed |
      | 01 January 2026 | Disbursement | 100.0             | 100.0            | 0.0               | 0.0                   | false    |
# --- add discount after disbursement on diff from same disbursement date should outcome with an error --- #
    When Admin sets the business date to "08 January 2026"
    Then Add Discount fee with "10" amount on Working Capital loan account failed due to date diff from disbursement date
    And Working capital loan account has the correct data:
      | product.name | submittedOnDate | expectedDisbursementDate | status | principal | approvedPrincipal | totalPayment | periodPaymentRate | discount |
      | WCLP         | 2026-01-01      | 2026-01-01               | Active | 100.0     | 100.0             | 100.0        | 1.0               | null     |
    And Working Capital Loan has transactions:
      | transactionDate | type         | transactionAmount | principalPortion | feeChargesPortion | penaltyChargesPortion | reversed |
      | 01 January 2026 | Disbursement | 100.0             | 100.0            | 0.0               | 0.0                   | false    |

  @TestRailId:C72395
  Scenario: Discount update on WCL account on the same as disburse date failed as already added discount before while create loan - UC3
    When Admin sets the business date to "01 January 2026"
    And Admin creates a client with random data
    And Admin creates a working capital loan with the following data:
      | LoanProduct | submittedOnDate | expectedDisbursementDate | principalAmount | totalPayment | periodPaymentRate | discount |
      | WCLP        | 01 January 2026 | 01 January 2026          | 100             | 100          | 1                 | 15       |
    Then Working capital loan creation was successful
    And Working capital loan account has the correct data:
      | product.name | submittedOnDate | expectedDisbursementDate | status                         | principal | approvedPrincipal | totalPayment | periodPaymentRate | discount |
      | WCLP         | 2026-01-01      | 2026-01-01               | Submitted and pending approval | 100.0     | 0.0               | 100.0        | 1.0               | 15.0     |
    Then Admin failed to approve the working capital loan on "01 January 2026" with "100" amount and expected disbursement date on "01 January 2026" with "18" exceeded discount amount
    Then Admin successfully approves the working capital loan on "01 January 2026" with "100" amount and "14" discount amount and expected disbursement date on "01 January 2026"
    Then Working capital loan approval was successful
    And Working capital loan account has the correct data:
      | product.name | submittedOnDate | expectedDisbursementDate | status   | principal | approvedPrincipal | totalPayment | periodPaymentRate | discount |
      | WCLP         | 2026-01-01      | 2026-01-01               | Approved | 100.0     | 100.0             | 100.0        | 1.0               | 14.0     |
    Then Admin failed to disburse the working capital loan on "01 January 2026" with "100" amount with "15" exceeded discount amount
    Then Admin successfully disburse the Working Capital loan on "01 January 2026" with "100" EUR transaction amount and "13" discount amount
    Then Working Capital loan status will be "ACTIVE"
    Then Verify Working Capital loan disbursement was successful
    And Working capital loan account has the correct data:
      | product.name | submittedOnDate | expectedDisbursementDate | status | principal | approvedPrincipal | totalPayment | periodPaymentRate | discount |
      | WCLP         | 2026-01-01      | 2026-01-01               | Active | 113.0     | 100.0             | 100.0        | 1.0               | 13.0     |
    And Working Capital Loan has transactions:
      | transactionDate | type         | transactionAmount | principalPortion | feeChargesPortion | penaltyChargesPortion | reversed |
      | 01 January 2026 | Disbursement | 100.0             | 100.0            | 0.0               | 0.0                   | false    |
      | 01 January 2026 | Discount Fee | 13.0              | 13.0             | 0.0               | 0.0                   | false    |
    Then Add Discount fee with "10" amount on Working Capital loan account failed due to already added discount before disbursement
    And Working Capital Loan has transactions:
      | transactionDate | type         | transactionAmount | principalPortion | feeChargesPortion | penaltyChargesPortion | reversed |
      | 01 January 2026 | Disbursement | 100.0             | 100.0            | 0.0               | 0.0                   | false    |
      | 01 January 2026 | Discount Fee | 13.0              | 13.0             | 0.0               | 0.0                   | false    |

  @TestRailId:C72396
  Scenario: Discount update on WCL account on the same as disburse date failed as already added discount before while modify loan - UC4
    When Admin sets the business date to "01 January 2026"
    And Admin creates a client with random data
    And Admin creates a working capital loan with the following data:
      | LoanProduct | submittedOnDate | expectedDisbursementDate | principalAmount | totalPayment | periodPaymentRate | discount |
      | WCLP        | 01 January 2026 | 01 January 2026          | 100             | 100          | 1                 |          |
    Then Working capital loan creation was successful
    And Working capital loan account has the correct data:
      | product.name | submittedOnDate | expectedDisbursementDate | status                         | principal | approvedPrincipal | totalPayment | periodPaymentRate | discount |
      | WCLP         | 2026-01-01      | 2026-01-01               | Submitted and pending approval | 100.0     | 0.0               | 100.0        | 1.0               | null     |
    When Admin modifies the working capital loan with the following data:
      | submittedOnDate | expectedDisbursementDate | principalAmount | totalPayment | periodPaymentRate | discount |
      |                 |                          |                 |              |                   | 15.0     |
    And Working capital loan account has the correct data:
      | product.name | submittedOnDate | expectedDisbursementDate | status                         | principal | approvedPrincipal | totalPayment | periodPaymentRate | discount |
      | WCLP         | 2026-01-01      | 2026-01-01               | Submitted and pending approval | 100.0     | 0.0               | 100.0        | 1.0               | 15.0     |
    Then Admin failed to approve the working capital loan on "01 January 2026" with "100" amount and expected disbursement date on "01 January 2026" with "18" exceeded discount amount
    Then Admin successfully approves the working capital loan on "01 January 2026" with "100" amount and "14" discount amount and expected disbursement date on "01 January 2026"
    Then Working capital loan approval was successful
    And Working capital loan account has the correct data:
      | product.name | submittedOnDate | expectedDisbursementDate | status   | principal | approvedPrincipal | totalPayment | periodPaymentRate | discount |
      | WCLP         | 2026-01-01      | 2026-01-01               | Approved | 100.0     | 100.0             | 100.0        | 1.0               | 14.0     |
    Then Admin failed to disburse the working capital loan on "01 January 2026" with "100" amount with "15" exceeded discount amount
    Then Admin successfully disburse the Working Capital loan on "01 January 2026" with "100" EUR transaction amount and "13" discount amount
    Then Working Capital loan status will be "ACTIVE"
    Then Verify Working Capital loan disbursement was successful
    And Working capital loan account has the correct data:
      | product.name | submittedOnDate | expectedDisbursementDate | status | principal | approvedPrincipal | totalPayment | periodPaymentRate | discount |
      | WCLP         | 2026-01-01      | 2026-01-01               | Active | 113.0     | 100.0             | 100.0        | 1.0               | 13.0     |
    And Working Capital Loan has transactions:
      | transactionDate | type         | transactionAmount | principalPortion | feeChargesPortion | penaltyChargesPortion | reversed |
      | 01 January 2026 | Disbursement | 100.0             | 100.0            | 0.0               | 0.0                   | false    |
      | 01 January 2026 | Discount Fee | 13.0              | 13.0             | 0.0               | 0.0                   | false    |
    Then Add Discount fee with "10" amount on Working Capital loan account failed due to already added discount before disbursement
    And Working Capital Loan has transactions:
      | transactionDate | type         | transactionAmount | principalPortion | feeChargesPortion | penaltyChargesPortion | reversed |
      | 01 January 2026 | Disbursement | 100.0             | 100.0            | 0.0               | 0.0                   | false    |
      | 01 January 2026 | Discount Fee | 13.0              | 13.0             | 0.0               | 0.0                   | false    |

  @TestRailId:C72397
  Scenario: Discount update on WCL account on the same as disburse date failed as already added discount before while approve loan - UC5
    When Admin sets the business date to "01 January 2026"
    And Admin creates a client with random data
    And Admin creates a working capital loan with the following data:
      | LoanProduct | submittedOnDate | expectedDisbursementDate | principalAmount | totalPayment | periodPaymentRate | discount |
      | WCLP        | 01 January 2026 | 01 January 2026          | 100             | 100          | 1                 |          |
    Then Working capital loan creation was successful
    And Working capital loan account has the correct data:
      | product.name | submittedOnDate | expectedDisbursementDate | status                         | principal | approvedPrincipal | totalPayment | periodPaymentRate | discount |
      | WCLP         | 2026-01-01      | 2026-01-01               | Submitted and pending approval | 100.0     | 0.0               | 100.0        | 1.0               | null     |
    Then Admin successfully approves the working capital loan on "01 January 2026" with "100" amount and "14" discount amount and expected disbursement date on "01 January 2026"
    Then Working capital loan approval was successful
    And Working capital loan account has the correct data:
      | product.name | submittedOnDate | expectedDisbursementDate | status   | principal | approvedPrincipal | totalPayment | periodPaymentRate | discount |
      | WCLP         | 2026-01-01      | 2026-01-01               | Approved | 100.0     | 100.0             | 100.0        | 1.0               | 14.0     |
    Then Admin successfully disburse the Working Capital loan on "01 January 2026" with "100" EUR transaction amount
    Then Working Capital loan status will be "ACTIVE"
    Then Verify Working Capital loan disbursement was successful
    And Working capital loan account has the correct data:
      | product.name | submittedOnDate | expectedDisbursementDate | status | principal | approvedPrincipal | totalPayment | periodPaymentRate | discount |
      | WCLP         | 2026-01-01      | 2026-01-01               | Active | 114.0     | 100.0             | 100.0        | 1.0               | 14.0     |
    And Working Capital Loan has transactions:
      | transactionDate | type         | transactionAmount | principalPortion | feeChargesPortion | penaltyChargesPortion | reversed |
      | 01 January 2026 | Disbursement | 100.0             | 100.0            | 0.0               | 0.0                   | false    |
      | 01 January 2026 | Discount Fee | 14.0              | 14.0             | 0.0               | 0.0                   | false    |
    Then Add Discount fee with "10" amount on Working Capital loan account failed due to already added discount before disbursement
    And Working Capital Loan has transactions:
      | transactionDate | type         | transactionAmount | principalPortion | feeChargesPortion | penaltyChargesPortion | reversed |
      | 01 January 2026 | Disbursement | 100.0             | 100.0            | 0.0               | 0.0                   | false    |
      | 01 January 2026 | Discount Fee | 14.0              | 14.0             | 0.0               | 0.0                   | false    |

  @TestRailId:C72490
  Scenario: Discount update od WCL loan account on the same as disburse date failed as already added discount before while disburse loan - UC6
    When Admin sets the business date to "01 January 2026"
    And Admin creates a client with random data
    And Admin creates a working capital loan with the following data:
      | LoanProduct | submittedOnDate | expectedDisbursementDate | principalAmount | totalPayment | periodPaymentRate | discount |
      | WCLP        | 01 January 2026 | 01 January 2026          | 100             | 100          | 1                 |          |
    Then Working capital loan creation was successful
    And Working capital loan account has the correct data:
      | product.name | submittedOnDate | expectedDisbursementDate | status                         | principal | approvedPrincipal | totalPayment | periodPaymentRate | discount |
      | WCLP         | 2026-01-01      | 2026-01-01               | Submitted and pending approval | 100.0     | 0.0               | 100.0        | 1.0               | null     |
    Then Admin successfully approves the working capital loan on "01 January 2026" with "100" amount and expected disbursement date on "01 January 2026"
    Then Working capital loan approval was successful
    And Working capital loan account has the correct data:
      | product.name | submittedOnDate | expectedDisbursementDate | status   | principal | approvedPrincipal | totalPayment | periodPaymentRate | discount |
      | WCLP         | 2026-01-01      | 2026-01-01               | Approved | 100.0     | 100.0             | 100.0        | 1.0               | null     |
    Then Admin successfully disburse the Working Capital loan on "01 January 2026" with "100" EUR transaction amount and "13" discount amount
    Then Working Capital loan status will be "ACTIVE"
    Then Verify Working Capital loan disbursement was successful
    And Working capital loan account has the correct data:
      | product.name | submittedOnDate | expectedDisbursementDate | status | principal | approvedPrincipal | totalPayment | periodPaymentRate | discount |
      | WCLP         | 2026-01-01      | 2026-01-01               | Active | 113.0     | 100.0             | 100.0        | 1.0               | 13.0     |
    And Working Capital Loan has transactions:
      | transactionDate | type         | transactionAmount | principalPortion | feeChargesPortion | penaltyChargesPortion | reversed |
      | 01 January 2026 | Disbursement | 100.0             | 100.0            | 0.0               | 0.0                   | false    |
      | 01 January 2026 | Discount Fee | 13.0              | 13.0             | 0.0               | 0.0                   | false    |
    Then Add Discount fee with "10" amount on Working Capital loan account failed due to already added discount before disbursement
    And Working Capital Loan has transactions:
      | transactionDate | type         | transactionAmount | principalPortion | feeChargesPortion | penaltyChargesPortion | reversed |
      | 01 January 2026 | Disbursement | 100.0             | 100.0            | 0.0               | 0.0                   | false    |
      | 01 January 2026 | Discount Fee | 13.0              | 13.0             | 0.0               | 0.0                   | false    |

  @TestRailId:C74491
  Scenario: Discount is forbidden to be added after disbursement as discount is set on loan product level with WCLP overrides disallowed - UC7
    When Admin sets the business date to "01 January 2026"
    And Admin creates a client with random data
    And Admin creates a working capital loan with the following data:
      | LoanProduct                                | submittedOnDate | expectedDisbursementDate | principalAmount | totalPayment | periodPaymentRate | discount |
      | WCLP_DISCOUNT_DISALLOW_ATTRIBUTES_OVERRIDE | 01 January 2026 | 01 January 2026          | 100             | 100          | 1                 |          |
    Then Working capital loan creation was successful
    And Working capital loan account has the correct data:
      | product.name                               | submittedOnDate | expectedDisbursementDate | status                         | principal | approvedPrincipal | totalPayment | periodPaymentRate | discount |
      | WCLP_DISCOUNT_DISALLOW_ATTRIBUTES_OVERRIDE | 2026-01-01      | 2026-01-01               | Submitted and pending approval | 100.0     | 0.0               | 100.0        | 1.0               | 50.0     |
    Then Admin failed to approve the working capital loan on "01 January 2026" with "100" amount and expected disbursement date on "01 January 2026" with "20" discount amount due to override disallowed by product
    Then Admin successfully approves the working capital loan on "01 January 2026" with "100" amount and expected disbursement date on "01 January 2026"
    Then Working capital loan approval was successful
    And Working capital loan account has the correct data:
      | product.name                               | submittedOnDate | expectedDisbursementDate | status   | principal | approvedPrincipal | totalPayment | periodPaymentRate | discount |
      | WCLP_DISCOUNT_DISALLOW_ATTRIBUTES_OVERRIDE | 2026-01-01      | 2026-01-01               | Approved | 100.0     | 100.0             | 100.0        | 1.0               | 50.0     |
    Then Admin failed to disburse the working capital loan on "01 January 2026" with "100" amount with "15" discount amount due to override disallowed by product
    Then Admin successfully disburse the Working Capital loan on "01 January 2026" with "100" EUR transaction amount
    Then Working Capital loan status will be "ACTIVE"
    Then Verify Working Capital loan disbursement was successful
    And Working capital loan account has the correct data:
      | product.name                               | submittedOnDate | expectedDisbursementDate | status | principal | approvedPrincipal | totalPayment | periodPaymentRate | discount |
      | WCLP_DISCOUNT_DISALLOW_ATTRIBUTES_OVERRIDE | 2026-01-01      | 2026-01-01               | Active | 150.0     | 100.0             | 100.0        | 1.0               | 50.0     |
    And Working Capital Loan has transactions:
      | transactionDate | type         | transactionAmount | principalPortion | feeChargesPortion | penaltyChargesPortion | reversed |
      | 01 January 2026 | Disbursement | 100.0             | 100.0            | 0.0               | 0.0                   | false    |
      | 01 January 2026 | Discount Fee | 50.0              | 50.0             | 0.0               | 0.0                   | false    |
# --- add discount after disbursement is forbidden due to overrides disallowed --- #
    And Add Discount fee with "20" amount on Working Capital loan account failed due to override disallowed by product
    And Working Capital Loan has transactions:
      | transactionDate | type         | transactionAmount | principalPortion | feeChargesPortion | penaltyChargesPortion | reversed |
      | 01 January 2026 | Disbursement | 100.0             | 100.0            | 0.0               | 0.0                   | false    |
      | 01 January 2026 | Discount Fee | 50.0              | 50.0             | 0.0               | 0.0                   | false    |

  @TestRailId:C74492
  Scenario: Discount is forbidden to be added after disbursement as NO discount is set on loan product level with WCLP overrides disallowed - UC8
    When Admin sets the business date to "01 January 2026"
    And Admin creates a client with random data
    And Admin creates a working capital loan with the following data:
      | LoanProduct                       | submittedOnDate | expectedDisbursementDate | principalAmount | totalPayment | periodPaymentRate | discount |
      | WCLP_DISALLOW_ATTRIBUTES_OVERRIDE | 01 January 2026 | 01 January 2026          | 100             | 100          | 1                 |          |
    Then Working capital loan creation was successful
    And Working capital loan account has the correct data:
      | product.name                      | submittedOnDate | expectedDisbursementDate | status                         | principal | approvedPrincipal | totalPayment | periodPaymentRate | discount |
      | WCLP_DISALLOW_ATTRIBUTES_OVERRIDE | 2026-01-01      | 2026-01-01               | Submitted and pending approval | 100.0     | 0.0               | 100.0        | 1.0               | null     |
    Then Admin failed to approve the working capital loan on "01 January 2026" with "100" amount and expected disbursement date on "01 January 2026" with "20" discount amount due to override disallowed by product
    Then Admin successfully approves the working capital loan on "01 January 2026" with "100" amount and expected disbursement date on "01 January 2026"
    Then Working capital loan approval was successful
    And Working capital loan account has the correct data:
      | product.name                      | submittedOnDate | expectedDisbursementDate | status   | principal | approvedPrincipal | totalPayment | periodPaymentRate | discount |
      | WCLP_DISALLOW_ATTRIBUTES_OVERRIDE | 2026-01-01      | 2026-01-01               | Approved | 100.0     | 100.0             | 100.0        | 1.0               | null     |
    Then Admin failed to disburse the working capital loan on "01 January 2026" with "100" amount with "20" discount amount due to override disallowed by product
    Then Admin successfully disburse the Working Capital loan on "01 January 2026" with "100" EUR transaction amount
    Then Working Capital loan status will be "ACTIVE"
    Then Verify Working Capital loan disbursement was successful
    And Working capital loan account has the correct data:
      | product.name                      | submittedOnDate | expectedDisbursementDate | status | principal | approvedPrincipal | totalPayment | periodPaymentRate | discount |
      | WCLP_DISALLOW_ATTRIBUTES_OVERRIDE | 2026-01-01      | 2026-01-01               | Active | 100.0     | 100.0             | 100.0        | 1.0               | null     |
    And Working Capital Loan has transactions:
      | transactionDate | type         | transactionAmount | principalPortion | feeChargesPortion | penaltyChargesPortion | reversed |
      | 01 January 2026 | Disbursement | 100.0             | 100.0            | 0.0               | 0.0                   | false    |
# --- add discount after disbursement is forbidden due to overrides disallowed --- #
    And Add Discount fee with "20" amount on Working Capital loan account failed due to override disallowed by product
    And Working Capital Loan has transactions:
      | transactionDate | type         | transactionAmount | principalPortion | feeChargesPortion | penaltyChargesPortion | reversed |
      | 01 January 2026 | Disbursement | 100.0             | 100.0            | 0.0               | 0.0                   | false    |

  @TestRailId:C74493
  Scenario: Discount is disallowed to be added after disbursement as discount is set and inherited from WC loan product level with WCLP overrides allowed - UC9
    When Admin sets the business date to "01 January 2026"
    And Admin creates a client with random data
    And Admin creates a working capital loan with the following data:
      | LoanProduct   | submittedOnDate | expectedDisbursementDate | principalAmount | totalPayment | periodPaymentRate | discount |
      | WCLP_DISCOUNT | 01 January 2026 | 01 January 2026          | 100             | 100          | 1                 |          |
    Then Working capital loan creation was successful
    And Working capital loan account has the correct data:
      | product.name  | submittedOnDate | expectedDisbursementDate | status                         | principal | approvedPrincipal | totalPayment | periodPaymentRate | discount |
      | WCLP_DISCOUNT | 2026-01-01      | 2026-01-01               | Submitted and pending approval | 100.0     | 0.0               | 100.0        | 1.0               | 50.0     |
    Then Admin successfully approves the working capital loan on "01 January 2026" with "100" amount and expected disbursement date on "01 January 2026"
    Then Working capital loan approval was successful
    And Working capital loan account has the correct data:
      | product.name  | submittedOnDate | expectedDisbursementDate | status   | principal | approvedPrincipal | totalPayment | periodPaymentRate | discount |
      | WCLP_DISCOUNT | 2026-01-01      | 2026-01-01               | Approved | 100.0     | 100.0             | 100.0        | 1.0               | 50.0     |
    Then Admin successfully disburse the Working Capital loan on "01 January 2026" with "100" EUR transaction amount
    Then Working Capital loan status will be "ACTIVE"
    Then Verify Working Capital loan disbursement was successful
    And Working capital loan account has the correct data:
      | product.name  | submittedOnDate | expectedDisbursementDate | status | principal | approvedPrincipal | totalPayment | periodPaymentRate | discount |
      | WCLP_DISCOUNT | 2026-01-01      | 2026-01-01               | Active | 150.0     | 100.0             | 100.0        | 1.0               | 50.0     |
    And Working Capital Loan has transactions:
      | transactionDate | type         | transactionAmount | principalPortion | feeChargesPortion | penaltyChargesPortion | reversed |
      | 01 January 2026 | Disbursement | 100               | 100              | 0                 | 0                     | false    |
      | 01 January 2026 | Discount Fee | 50                | 50               | 0                 | 0                     | false    |
 # --- add discount after disbursement is forbidden as discount is already added --- #
    Then Add Discount fee with "10" amount on Working Capital loan account failed due to already added discount before disbursement
    And Working Capital Loan has transactions:
      | transactionDate | type         | transactionAmount | principalPortion | feeChargesPortion | penaltyChargesPortion | reversed |
      | 01 January 2026 | Disbursement | 100.0             | 100.0            | 0.0               | 0.0                   | false    |
      | 01 January 2026 | Discount Fee | 50                | 50               | 0                 | 0                     | false    |

  @TestRailId:C72494
  Scenario: Discount is forbidden to be added after Modify with modified discount amount allowed as discount is set on loan product level with WCLP overrides allowed - UC10
    When Admin sets the business date to "01 January 2026"
    And Admin creates a client with random data
    And Admin creates a working capital loan with the following data:
      | LoanProduct   | submittedOnDate | expectedDisbursementDate | principalAmount | totalPayment | periodPaymentRate | discount |
      | WCLP_DISCOUNT | 01 January 2026 | 01 January 2026          | 100             | 100          | 1                 | 0       |
    Then Working capital loan creation was successful
    And Working capital loan account has the correct data:
      | product.name  | submittedOnDate | expectedDisbursementDate | status                         | principal | approvedPrincipal | totalPayment | periodPaymentRate | discount |
      | WCLP_DISCOUNT | 2026-01-01      | 2026-01-01               | Submitted and pending approval | 100.0     | 0.0               | 100.0        | 1.0               | 0.0      |
    When Admin modifies the working capital loan with the following data:
      | submittedOnDate | expectedDisbursementDate | principalAmount | totalPayment | periodPaymentRate | discount |
      |                 |                          |                 |              |                   | 55.0     |
    Then Admin failed to approve the working capital loan on "01 January 2026" with "100" amount and expected disbursement date on "01 January 2026" with "60" exceeded discount amount
    Then Admin successfully approves the working capital loan on "01 January 2026" with "100" amount and expected disbursement date on "01 January 2026"
    Then Working capital loan approval was successful
    And Working capital loan account has the correct data:
      | product.name  | submittedOnDate | expectedDisbursementDate | status   | principal | approvedPrincipal | totalPayment | periodPaymentRate | discount |
      | WCLP_DISCOUNT | 2026-01-01      | 2026-01-01               | Approved | 100.0     | 100.0             | 100.0        | 1.0               | 55.0     |
    Then Admin failed to disburse the working capital loan on "01 January 2026" with "100" amount with "60" exceeded discount amount
    Then Admin successfully disburse the Working Capital loan on "01 January 2026" with "100" EUR transaction amount
    Then Working Capital loan status will be "ACTIVE"
    Then Verify Working Capital loan disbursement was successful
    And Working capital loan account has the correct data:
      | product.name  | submittedOnDate | expectedDisbursementDate | status | principal | approvedPrincipal | totalPayment | periodPaymentRate | discount |
      | WCLP_DISCOUNT | 2026-01-01      | 2026-01-01               | Active | 155.0     | 100.0             | 100.0        | 1.0               | 55.0     |
    And Working Capital Loan has transactions:
      | transactionDate | type         | transactionAmount | principalPortion | feeChargesPortion | penaltyChargesPortion | reversed |
      | 01 January 2026 | Disbursement | 100.0             | 100.0            | 0.0               | 0.0                   | false    |
      | 01 January 2026 | Discount Fee | 55.0              | 55.0             | 0                 | 0                     | false    |
# --- add discount after disbursement is forbidden as discount is already added and updated on disbursement --- #
    Then Add Discount fee with "10" amount on Working Capital loan account failed due to already added discount before disbursement
    And Working Capital Loan has transactions:
      | transactionDate | type         | transactionAmount | principalPortion | feeChargesPortion | penaltyChargesPortion | reversed |
      | 01 January 2026 | Disbursement | 100.0             | 100.0            | 0.0               | 0.0                   | false    |
      | 01 January 2026 | Discount Fee | 55.0              | 55.0             | 0                 | 0                     | false    |

  @TestRailId:C78836
  Scenario: Discount is forbidden to be added after Approve with modified discount amount allowed as discount is set on loan product level with WCLP overrides allowed - UC11
    When Admin sets the business date to "01 January 2026"
    And Admin creates a client with random data
    And Admin creates a working capital loan with the following data:
      | LoanProduct   | submittedOnDate | expectedDisbursementDate | principalAmount | totalPayment | periodPaymentRate | discount |
      | WCLP_DISCOUNT | 01 January 2026 | 01 January 2026          | 100             | 100          | 1                 |          |
    Then Working capital loan creation was successful
    And Working capital loan account has the correct data:
      | product.name  | submittedOnDate | expectedDisbursementDate | status                         | principal | approvedPrincipal | totalPayment | periodPaymentRate | discount |
      | WCLP_DISCOUNT | 2026-01-01      | 2026-01-01               | Submitted and pending approval | 100.0     | 0.0               | 100.0        | 1.0               | 50.0     |
    Then Admin failed to approve the working capital loan on "01 January 2026" with "100" amount and expected disbursement date on "01 January 2026" with "60" exceeded discount amount
    Then Admin successfully approves the working capital loan on "01 January 2026" with "100" amount and "40" discount amount and expected disbursement date on "01 January 2026"
    Then Working capital loan approval was successful
    And Working capital loan account has the correct data:
      | product.name  | submittedOnDate | expectedDisbursementDate | status   | principal | approvedPrincipal | totalPayment | periodPaymentRate | discount |
      | WCLP_DISCOUNT | 2026-01-01      | 2026-01-01               | Approved | 100.0     | 100.0             | 100.0        | 1.0               | 40.0     |
    Then Admin failed to disburse the working capital loan on "01 January 2026" with "100" amount with "50" exceeded discount amount
    Then Admin successfully disburse the Working Capital loan on "01 January 2026" with "100" EUR transaction amount
    Then Working Capital loan status will be "ACTIVE"
    Then Verify Working Capital loan disbursement was successful
    And Working capital loan account has the correct data:
      | product.name  | submittedOnDate | expectedDisbursementDate | status | principal | approvedPrincipal | totalPayment | periodPaymentRate | discount |
      | WCLP_DISCOUNT | 2026-01-01      | 2026-01-01               | Active | 140.0     | 100.0             | 100.0        | 1.0               | 40.0     |
    And Working Capital Loan has transactions:
      | transactionDate | type         | transactionAmount | principalPortion | feeChargesPortion | penaltyChargesPortion | reversed |
      | 01 January 2026 | Disbursement | 100.0             | 100.0            | 0.0               | 0.0                   | false    |
      | 01 January 2026 | Discount Fee | 40.0              | 40.0             | 0                 | 0                     | false    |
  # --- add discount after disbursement is forbidden as discount is already added --- #
    Then Add Discount fee with "10" amount on Working Capital loan account failed due to already added discount before disbursement
    And Working Capital Loan has transactions:
      | transactionDate | type         | transactionAmount | principalPortion | feeChargesPortion | penaltyChargesPortion | reversed |
      | 01 January 2026 | Disbursement | 100.0             | 100.0            | 0.0               | 0.0                   | false    |
      | 01 January 2026 | Discount Fee | 40.0              | 40.0             | 0                 | 0                     | false    |

  @TestRailId:C78837
  Scenario: Discount is forbidden to be added after Disburse with modified discount amount allowed as discount is set on loan product level with WCLP overrides allowed - UC12
    When Admin sets the business date to "01 January 2026"
    And Admin creates a client with random data
    And Admin creates a working capital loan with the following data:
      | LoanProduct   | submittedOnDate | expectedDisbursementDate | principalAmount | totalPayment | periodPaymentRate | discount |
      | WCLP_DISCOUNT | 01 January 2026 | 01 January 2026          | 100             | 100          | 1                 |          |
    Then Working capital loan creation was successful
    And Working capital loan account has the correct data:
      | product.name  | submittedOnDate | expectedDisbursementDate | status                         | principal | approvedPrincipal | totalPayment | periodPaymentRate | discount |
      | WCLP_DISCOUNT | 2026-01-01      | 2026-01-01               | Submitted and pending approval | 100.0     | 0.0               | 100.0        | 1.0               | 50.0     |
    Then Admin failed to approve the working capital loan on "01 January 2026" with "100" amount and expected disbursement date on "01 January 2026" with "60" exceeded discount amount
    Then Admin successfully approves the working capital loan on "01 January 2026" with "100" amount and expected disbursement date on "01 January 2026"
    Then Working capital loan approval was successful
    And Working capital loan account has the correct data:
      | product.name  | submittedOnDate | expectedDisbursementDate | status   | principal | approvedPrincipal | totalPayment | periodPaymentRate | discount |
      | WCLP_DISCOUNT | 2026-01-01      | 2026-01-01               | Approved | 100.0     | 100.0             | 100.0        | 1.0               | 50.0     |
    Then Admin failed to disburse the working capital loan on "01 January 2026" with "100" amount with "60" exceeded discount amount
    Then Admin successfully disburse the Working Capital loan on "01 January 2026" with "100" EUR transaction amount and "45" discount amount
    Then Working Capital loan status will be "ACTIVE"
    Then Verify Working Capital loan disbursement was successful
    And Working capital loan account has the correct data:
      | product.name  | submittedOnDate | expectedDisbursementDate | status | principal | approvedPrincipal | totalPayment | periodPaymentRate | discount |
      | WCLP_DISCOUNT | 2026-01-01      | 2026-01-01               | Active | 145.0     | 100.0             | 100.0        | 1.0               | 45.0     |
    And Working Capital Loan has transactions:
      | transactionDate | type         | transactionAmount | principalPortion | feeChargesPortion | penaltyChargesPortion | reversed |
      | 01 January 2026 | Disbursement | 100.0             | 100.0            | 0.0               | 0.0                   | false    |
      | 01 January 2026 | Discount Fee | 45.0              | 45.0             | 0                 | 0                     | false    |
# --- add discount after disbursement is forbidden as discount is already added and updated on disbursement --- #
    Then Add Discount fee with "10" amount on Working Capital loan account failed due to already added discount before disbursement
    And Working Capital Loan has transactions:
      | transactionDate | type         | transactionAmount | principalPortion | feeChargesPortion | penaltyChargesPortion | reversed |
      | 01 January 2026 | Disbursement | 100.0             | 100.0            | 0.0               | 0.0                   | false    |
      | 01 January 2026 | Discount Fee | 45.0              | 45.0             | 0                 | 0                     | false    |

  @TestRailId:C78838
  Scenario: Discount is allowed to be added after disbursement as overridden with NO discount value from defined on WCLP on loan Create with WCLP overrides allowed - UC13
    When Admin sets the business date to "01 January 2026"
    And Admin creates a client with random data
    And Admin creates a working capital loan with the following data:
      | LoanProduct   | submittedOnDate | expectedDisbursementDate | principalAmount | totalPayment | periodPaymentRate | discount |
      | WCLP_DISCOUNT | 01 January 2026 | 01 January 2026          | 100             | 100          | 1                 | 0        |
    Then Working capital loan creation was successful
    And Working capital loan account has the correct data:
      | product.name  | submittedOnDate | expectedDisbursementDate | status                         | principal | approvedPrincipal | totalPayment | periodPaymentRate | discount |
      | WCLP_DISCOUNT | 2026-01-01      | 2026-01-01               | Submitted and pending approval | 100.0     | 0.0               | 100.0        | 1.0               | 0.0      |
    Then Admin successfully approves the working capital loan on "01 January 2026" with "100" amount and expected disbursement date on "01 January 2026"
    Then Working capital loan approval was successful
    And Working capital loan account has the correct data:
      | product.name  | submittedOnDate | expectedDisbursementDate | status   | principal | approvedPrincipal | totalPayment | periodPaymentRate | discount |
      | WCLP_DISCOUNT | 2026-01-01      | 2026-01-01               | Approved | 100.0     | 100.0             | 100.0        | 1.0               | 0.0      |
    Then Admin successfully disburse the Working Capital loan on "01 January 2026" with "100" EUR transaction amount
    Then Working Capital loan status will be "ACTIVE"
    Then Verify Working Capital loan disbursement was successful
    And Working capital loan account has the correct data:
      | product.name  | submittedOnDate | expectedDisbursementDate | status | principal | approvedPrincipal | totalPayment | periodPaymentRate | discount |
      | WCLP_DISCOUNT | 2026-01-01      | 2026-01-01               | Active | 100.0     | 100.0             | 100.0        | 1.0               | 0.0      |
    And Working Capital Loan has transactions:
      | transactionDate | type         | transactionAmount | principalPortion | feeChargesPortion | penaltyChargesPortion | reversed |
      | 01 January 2026 | Disbursement | 100               | 100              | 0                 | 0                     | false    |
 # --- add discount after disbursement is allowed as NO discount is added yet --- #
    Then Admin adds Discount fee with "10" amount on Working Capital loan account for last disbursement
    And Working Capital Loan has transactions:
      | transactionDate | type         | transactionAmount | principalPortion | feeChargesPortion | penaltyChargesPortion | reversed |
      | 01 January 2026 | Disbursement | 100.0             | 100.0            | 0.0               | 0.0                   | false    |
      | 01 January 2026 | Discount Fee | 10.0              | 10.0             | 0.0               | 0.0                   | false    |

  @TestRailId:C78839
  Scenario: Working Capital Loan Transaction - Discount added after disbursement on disbursement undo - UC14
    When Admin sets the business date to "01 January 2026"
    And Admin creates a client with random data
    And Admin creates a working capital loan with the following data:
      | LoanProduct | submittedOnDate | expectedDisbursementDate | principalAmount | totalPayment | periodPaymentRate | discount |
      | WCLP        | 01 January 2026 | 01 January 2026          | 100.0           | 100.0        | 1.0               | 0.0      |
    Then Working capital loan creation was successful
    And Working capital loan account has the correct data:
      | product.name | submittedOnDate | expectedDisbursementDate | status                         | principal | approvedPrincipal | totalPayment | periodPaymentRate | discount |
      | WCLP         | 2026-01-01      | 2026-01-01               | Submitted and pending approval | 100.0     | 0.0               | 100.0        | 1.0               | 0.0      |
    When Admin successfully approves the working capital loan on "01 January 2026" with "100" amount and expected disbursement date on "01 January 2026"
    Then Working capital loan approval was successful
    And Working capital loan account has the correct data:
      | product.name | submittedOnDate | expectedDisbursementDate | status   | principal | approvedPrincipal | totalPayment | periodPaymentRate | discount |
      | WCLP         | 2026-01-01      | 2026-01-01               | Approved | 100.0     | 100.0             | 100.0        | 1.0               | 0.0      |
    Then Admin successfully disburse the Working Capital loan on "01 January 2026" with "100" EUR transaction amount
    Then Working Capital loan status will be "ACTIVE"
    Then Verify Working Capital loan disbursement was successful
    And Working capital loan account has the correct data:
      | product.name  | submittedOnDate | expectedDisbursementDate | status | principal | approvedPrincipal | totalPayment | periodPaymentRate | discount |
      | WCLP          | 2026-01-01      | 2026-01-01               | Active | 100.0     | 100.0             | 100.0        | 1.0               | 0.0     |
    And Working Capital Loan has transactions:
      | transactionDate | type         | transactionAmount | principalPortion | feeChargesPortion | penaltyChargesPortion | reversed |
      | 01 January 2026 | Disbursement | 100.0             | 100.0            | 0.0               | 0.0                   | false    |
    Then Admin adds Discount fee with "10" amount on Working Capital loan account for last disbursement
    And Working Capital Loan has transactions:
      | transactionDate | type         | transactionAmount | principalPortion | feeChargesPortion | penaltyChargesPortion | reversed |
      | 01 January 2026 | Disbursement | 100.0             | 100.0            | 0.0               | 0.0                   | false    |
      | 01 January 2026 | Discount Fee | 10.0              | 10.0             | 0.0               | 0.0                   | false    |
    And Admin successfully undo Working Capital disbursal by externalId
    And Working capital loan account has the correct data:
      | product.name | submittedOnDate | expectedDisbursementDate | status   | principal | approvedPrincipal | totalPayment | periodPaymentRate | discount |
      | WCLP         | 2026-01-01      | 2026-01-01               | Approved | 100.0     | 100.0             | 100.0        | 1.0               | 10.0     |
    And Working Capital Loan has transactions:
      | transactionDate | type         | transactionAmount | principalPortion | feeChargesPortion | penaltyChargesPortion | reversed |
      | 01 January 2026 | Disbursement | 100.0             | 100.0            | 0.0               | 0.0                   | true     |
      | 01 January 2026 | Discount Fee | 10.0              | 10.0             | 0.0               | 0.0                   | true     |

  @TestRailId:C78840
  Scenario: Working Capital Loan Transaction - Discount added on disbursement on disbursement undo - UC15
    When Admin sets the business date to "01 January 2026"
    And Admin creates a client with random data
    And Admin creates a working capital loan with the following data:
      | LoanProduct | submittedOnDate | expectedDisbursementDate | principalAmount | totalPayment | periodPaymentRate | discount |
      | WCLP        | 01 January 2026 | 01 January 2026          | 100.0           | 100.0        | 1.0               | 22.0      |
    Then Working capital loan creation was successful
    And Working capital loan account has the correct data:
      | product.name | submittedOnDate | expectedDisbursementDate | status                         | principal | approvedPrincipal | totalPayment | periodPaymentRate | discount |
      | WCLP         | 2026-01-01      | 2026-01-01               | Submitted and pending approval | 100.0     | 0.0               | 100.0        | 1.0               | 22.0      |
    When Admin successfully approves the working capital loan on "01 January 2026" with "100" amount and expected disbursement date on "01 January 2026"
    Then Working capital loan approval was successful
    And Working capital loan account has the correct data:
      | product.name | submittedOnDate | expectedDisbursementDate | status   | principal | approvedPrincipal | totalPayment | periodPaymentRate | discount |
      | WCLP         | 2026-01-01      | 2026-01-01               | Approved | 100.0     | 100.0             | 100.0        | 1.0               | 22.0      |
    Then Admin successfully disburse the Working Capital loan on "01 January 2026" with "100" EUR transaction amount and "11" discount amount
    Then Working Capital loan status will be "ACTIVE"
    Then Verify Working Capital loan disbursement was successful
    And Working capital loan account has the correct data:
      | product.name  | submittedOnDate | expectedDisbursementDate | status | principal | approvedPrincipal | totalPayment | periodPaymentRate | discount |
      | WCLP          | 2026-01-01      | 2026-01-01               | Active | 111.0     | 100.0             | 100.0        | 1.0               | 11.0     |
    And Working Capital Loan has transactions:
      | transactionDate | type         | transactionAmount | principalPortion | feeChargesPortion | penaltyChargesPortion | reversed |
      | 01 January 2026 | Disbursement | 100.0             | 100.0            | 0.0               | 0.0                   | false    |
      | 01 January 2026 | Discount Fee | 11.0              | 11.0             | 0.0               | 0.0                   | false    |
    And Admin successfully undo Working Capital disbursal by externalId
    And Working capital loan account has the correct data:
      | product.name | submittedOnDate | expectedDisbursementDate | status   | principal | approvedPrincipal | totalPayment | periodPaymentRate | discount |
      | WCLP         | 2026-01-01      | 2026-01-01               | Approved | 100.0     | 100.0             | 100.0        | 1.0               | 11.0     |
    And Working Capital Loan has transactions:
      | transactionDate | type         | transactionAmount | principalPortion | feeChargesPortion | penaltyChargesPortion | reversed |
      | 01 January 2026 | Disbursement | 100.0             | 100.0            | 0.0               | 0.0                   | true     |
      | 01 January 2026 | Discount Fee | 11.0              | 11.0             | 0.0               | 0.0                   | true     |

@WorkingCapitalNearBreachEvaluationFeature
Feature: Working Capital Near Breach Evaluation

  @TestRailId:C76635
  Scenario: Verify near breach detected when outstanding exceeds threshold at evaluation date
    When Admin sets the business date to "01 January 2026"
    And Admin creates a client with random data
    And Admin creates a Working Capital Loan Product with breach and near breach config and overrides enabled:
      | breachFrequency | breachFrequencyType | breachAmountCalculationType | breachAmount | nearBreachFrequency | nearBreachFrequencyType | nearBreachThreshold | delinquencyGraceDays |
      | 3               | MONTHS              | FLAT                        | 900          | 60                  | DAYS                    | 33.33               |                      |
    And Admin creates a working capital loan using created product with the following data:
      | submittedOnDate | expectedDisbursementDate | principalAmount | totalPayment | periodPaymentRate | discount |
      | 01 January 2026 | 01 January 2026          | 9000            | 100000       | 18                | 0        |
    And Admin successfully approves the working capital loan on "01 January 2026" with "9000" amount and expected disbursement date on "01 January 2026"
    When Admin successfully disburse the Working Capital loan on "01 January 2026" with "9000" EUR transaction amount
    And Admin runs inline COB job for Working Capital Loan by loanId
    # Period 1: 01-01 -> 03-31, freq=60d -> 1 eval at 03-02 (cumulative required = 33.33% of 900 = 299.97)
    # No payment by 03-02 -> cumulative paid=0 < 299.97 -> trigger Y
    When Admin sets the business date to "03 March 2026"
    And Admin runs inline COB job for Working Capital Loan by loanId
    Then Working Capital loan breach schedule has the following data:
      | periodNumber | fromDate   | toDate     | minPaymentAmount | outstandingAmount | nearBreach | breach |
      | 1            | 2026-01-01 | 2026-03-31 | 900.00           | 900.00            | true       | null   |

  @TestRailId:C76636
  Scenario: Verify near breach not triggered when payment brings outstanding below threshold before evaluation date
    When Admin sets the business date to "01 January 2026"
    And Admin creates a client with random data
    And Admin creates a Working Capital Loan Product with breach and near breach config and overrides enabled:
      | breachFrequency | breachFrequencyType | breachAmountCalculationType | breachAmount | nearBreachFrequency | nearBreachFrequencyType | nearBreachThreshold | delinquencyGraceDays |
      | 3               | MONTHS              | FLAT                        | 900          | 60                  | DAYS                    | 33.33               |                      |
    And Admin creates a working capital loan using created product with the following data:
      | submittedOnDate | expectedDisbursementDate | principalAmount | totalPayment | periodPaymentRate | discount |
      | 01 January 2026 | 01 January 2026          | 9000            | 100000       | 18                | 0        |
    And Admin successfully approves the working capital loan on "01 January 2026" with "9000" amount and expected disbursement date on "01 January 2026"
    When Admin successfully disburse the Working Capital loan on "01 January 2026" with "9000" EUR transaction amount
    And Admin runs inline COB job for Working Capital Loan by loanId
    # Period 1: 01-01 -> 03-31, freq=60d -> 1 eval at 03-02 (cumulative required = 299.97)
    # Pay 700 on 15 Feb -> cumulative paid by 03-02 = 700 >= 299.97 -> not trigger
    # After period end (31 Mar) -> nearBreach=false; outstanding=200>0 -> breach=true
    When Admin sets the business date to "15 February 2026"
    And Customer makes repayment on "15 February 2026" with 700.0 transaction amount on Working Capital loan
    When Admin sets the business date to "01 April 2026"
    And Admin runs inline COB job for Working Capital Loan by loanId
    Then Working Capital loan breach schedule has the following data:
      | periodNumber | fromDate   | toDate     | minPaymentAmount | outstandingAmount | nearBreach | breach |
      | 1            | 2026-01-01 | 2026-03-31 | 900.00           | 200.00            | false      | true   |
      | 2            | 2026-04-01 | 2026-06-30 | 900.00           | 900.00            | null       | null   |

  @TestRailId:C76637
  Scenario: Verify near breach null when no near breach config on product
    When Admin sets the business date to "01 January 2026"
    And Admin creates a client with random data
    And Admin creates a Working Capital Loan Product with custom breach config and overrides enabled:
      | breachFrequency | breachFrequencyType | breachAmountCalculationType | breachAmount | delinquencyGraceDays |
      | 1               | MONTHS              | FLAT                        | 500          |                      |
    And Admin creates a working capital loan using created product with the following data:
      | submittedOnDate | expectedDisbursementDate | principalAmount | totalPayment | periodPaymentRate | discount |
      | 01 January 2026 | 01 January 2026          | 9000            | 100000       | 18                | 0        |
    And Admin successfully approves the working capital loan on "01 January 2026" with "9000" amount and expected disbursement date on "01 January 2026"
    When Admin successfully disburse the Working Capital loan on "01 January 2026" with "9000" EUR transaction amount
    And Admin runs inline COB job for Working Capital Loan by loanId
    When Admin sets the business date to "01 February 2026"
    And Admin runs inline COB job for Working Capital Loan by loanId
    Then Working Capital loan breach schedule has the following data:
      | periodNumber | fromDate   | toDate     | minPaymentAmount | outstandingAmount | nearBreach | breach |
      | 1            | 2026-01-01 | 2026-01-31 | 500.00           | 500.00            | null       | true   |
      | 2            | 2026-02-01 | 2026-02-28 | 500.00           | 500.00            | null       | null   |

  @TestRailId:C76638
  Scenario: Verify near breach is immutable - stays true after subsequent payment
    When Admin sets the business date to "01 January 2026"
    And Admin creates a client with random data
    And Admin creates a Working Capital Loan Product with breach and near breach config and overrides enabled:
      | breachFrequency | breachFrequencyType | breachAmountCalculationType | breachAmount | nearBreachFrequency | nearBreachFrequencyType | nearBreachThreshold | delinquencyGraceDays |
      | 3               | MONTHS              | FLAT                        | 900          | 60                  | DAYS                    | 33.33               |                      |
    And Admin creates a working capital loan using created product with the following data:
      | submittedOnDate | expectedDisbursementDate | principalAmount | totalPayment | periodPaymentRate | discount |
      | 01 January 2026 | 01 January 2026          | 9000            | 100000       | 18                | 0        |
    And Admin successfully approves the working capital loan on "01 January 2026" with "9000" amount and expected disbursement date on "01 January 2026"
    When Admin successfully disburse the Working Capital loan on "01 January 2026" with "9000" EUR transaction amount
    And Admin runs inline COB job for Working Capital Loan by loanId
    # No payment by 03-02 -> cumulative paid=0 < 299.97 -> trigger Y at eval 03-02
    When Admin sets the business date to "03 March 2026"
    And Admin runs inline COB job for Working Capital Loan by loanId
    Then Working Capital loan breach schedule has the following data:
      | periodNumber | fromDate   | toDate     | minPaymentAmount | outstandingAmount | nearBreach | breach |
      | 1            | 2026-01-01 | 2026-03-31 | 900.00           | 900.00            | true       | null   |
    # Now pay full amount - near breach must stay true (immutable)
    When Admin sets the business date to "15 March 2026"
    And Customer makes repayment on "15 March 2026" with 900.0 transaction amount on Working Capital loan
    When Admin sets the business date to "01 April 2026"
    And Admin runs inline COB job for Working Capital Loan by loanId
    Then Working Capital loan breach schedule has the following data:
      | periodNumber | fromDate   | toDate     | minPaymentAmount | outstandingAmount | nearBreach | breach |
      | 1            | 2026-01-01 | 2026-03-31 | 900.00           | 0.00              | true       | false  |
      | 2            | 2026-04-01 | 2026-06-30 | 900.00           | 900.00            | null       | null   |

  @TestRailId:C76640
  Scenario: Verify near breach evaluation before eval date - near breach stays null
    When Admin sets the business date to "01 January 2026"
    And Admin creates a client with random data
    And Admin creates a Working Capital Loan Product with breach and near breach config and overrides enabled:
      | breachFrequency | breachFrequencyType | breachAmountCalculationType | breachAmount | nearBreachFrequency | nearBreachFrequencyType | nearBreachThreshold | delinquencyGraceDays |
      | 3               | MONTHS              | FLAT                        | 900          | 60                  | DAYS                    | 33.33               |                      |
    And Admin creates a working capital loan using created product with the following data:
      | submittedOnDate | expectedDisbursementDate | principalAmount | totalPayment | periodPaymentRate | discount |
      | 01 January 2026 | 01 January 2026          | 9000            | 100000       | 18                | 0        |
    And Admin successfully approves the working capital loan on "01 January 2026" with "9000" amount and expected disbursement date on "01 January 2026"
    When Admin successfully disburse the Working Capital loan on "01 January 2026" with "9000" EUR transaction amount
    And Admin runs inline COB job for Working Capital Loan by loanId
    # freq=60d -> 1 eval at 03-02. COB on 01 Mar -> evalDate not yet passed -> nearBreach stays null
    When Admin sets the business date to "01 March 2026"
    And Admin runs inline COB job for Working Capital Loan by loanId
    Then Working Capital loan breach schedule has the following data:
      | periodNumber | fromDate   | toDate     | minPaymentAmount | outstandingAmount | nearBreach | breach |
      | 1            | 2026-01-01 | 2026-03-31 | 900.00           | 900.00            | null       | null   |

  @TestRailId:C76641
  Scenario: Verify near breach with PERCENTAGE breach amount and WEEKS near breach frequency
    When Admin sets the business date to "01 January 2026"
    And Admin creates a client with random data
    And Admin creates a Working Capital Loan Product with breach and near breach config and overrides enabled:
      | breachFrequency | breachFrequencyType | breachAmountCalculationType | breachAmount | nearBreachFrequency | nearBreachFrequencyType | nearBreachThreshold | delinquencyGraceDays |
      | 2               | MONTHS              | PERCENTAGE                  | 10           | 2                   | WEEKS                   | 50                  |                      |
    And Admin creates a working capital loan using created product with the following data:
      | submittedOnDate | expectedDisbursementDate | principalAmount | totalPayment | periodPaymentRate | discount |
      | 01 January 2026 | 01 January 2026          | 9000            | 100000       | 18                | 0        |
    And Admin successfully approves the working capital loan on "01 January 2026" with "9000" amount and expected disbursement date on "01 January 2026"
    When Admin successfully disburse the Working Capital loan on "01 January 2026" with "9000" EUR transaction amount
    And Admin runs inline COB job for Working Capital Loan by loanId
    # Period 1: 01-01 -> 02-28 (2 months -1 day), minPayment=10% of 9000=900
    # freq=2 weeks -> 4 evals: 01-15, 01-29, 02-12, 02-26 (step required = 50% of 900 = 450)
    # No payment by eval#1 -> cumulative paid=0 < 450 -> trigger Y at eval#1
    When Admin sets the business date to "16 January 2026"
    And Admin runs inline COB job for Working Capital Loan by loanId
    Then Working Capital loan breach schedule has the following data:
      | periodNumber | fromDate   | toDate     | minPaymentAmount | outstandingAmount | nearBreach | breach |
      | 1            | 2026-01-01 | 2026-02-28 | 900.00           | 900.00            | true       | null   |

  @TestRailId:C76642
  Scenario: Verify near breach not triggered when outstanding equals threshold exactly - strict greater than
    When Admin sets the business date to "01 January 2026"
    And Admin creates a client with random data
    And Admin creates a Working Capital Loan Product with breach and near breach config and overrides enabled:
      | breachFrequency | breachFrequencyType | breachAmountCalculationType | breachAmount | nearBreachFrequency | nearBreachFrequencyType | nearBreachThreshold | delinquencyGraceDays |
      | 3               | MONTHS              | FLAT                        | 900          | 60                  | DAYS                    | 50                  |                      |
    And Admin creates a working capital loan using created product with the following data:
      | submittedOnDate | expectedDisbursementDate | principalAmount | totalPayment | periodPaymentRate | discount |
      | 01 January 2026 | 01 January 2026          | 9000            | 100000       | 18                | 0        |
    And Admin successfully approves the working capital loan on "01 January 2026" with "9000" amount and expected disbursement date on "01 January 2026"
    When Admin successfully disburse the Working Capital loan on "01 January 2026" with "9000" EUR transaction amount
    And Admin runs inline COB job for Working Capital Loan by loanId
    # freq=60d -> 1 eval at 03-02 (cumulative required = 50% of 900 = 450)
    # Pay 450 on 15 Jan -> cumulative paid=450; strict less-than means 450 is NOT below 450 -> not trigger
    # After period end -> nearBreach=false; outstanding=450>0 -> breach=true
    When Admin sets the business date to "15 January 2026"
    And Customer makes repayment on "15 January 2026" with 450.0 transaction amount on Working Capital loan
    When Admin sets the business date to "01 April 2026"
    And Admin runs inline COB job for Working Capital Loan by loanId
    Then Working Capital loan breach schedule has the following data:
      | periodNumber | fromDate   | toDate     | minPaymentAmount | outstandingAmount | nearBreach | breach |
      | 1            | 2026-01-01 | 2026-03-31 | 900.00           | 450.00            | false      | true   |
      | 2            | 2026-04-01 | 2026-06-30 | 900.00           | 900.00            | null       | null   |

  @TestRailId:C76643
  Scenario: Verify near breach evaluated independently per breach period
    When Admin sets the business date to "01 January 2026"
    And Admin creates a client with random data
    And Admin creates a Working Capital Loan Product with breach and near breach config and overrides enabled:
      | breachFrequency | breachFrequencyType | breachAmountCalculationType | breachAmount | nearBreachFrequency | nearBreachFrequencyType | nearBreachThreshold | delinquencyGraceDays |
      | 1               | MONTHS              | FLAT                        | 500          | 15                  | DAYS                    | 50                  |                      |
    And Admin creates a working capital loan using created product with the following data:
      | submittedOnDate | expectedDisbursementDate | principalAmount | totalPayment | periodPaymentRate | discount |
      | 01 January 2026 | 01 January 2026          | 9000            | 100000       | 18                | 0        |
    And Admin successfully approves the working capital loan on "01 January 2026" with "9000" amount and expected disbursement date on "01 January 2026"
    When Admin successfully disburse the Working Capital loan on "01 January 2026" with "9000" EUR transaction amount
    And Admin runs inline COB job for Working Capital Loan by loanId
    # Period 1: 01-01 -> 01-31, freq=15d -> 1 applicable eval at 01-16; 01-31 is the breach due date and excluded.
    # No payment in P1 by eval#1 -> cumulative paid=0 < 250 -> nearBreach=true at eval#1
    # Period 2: 02-01 -> 02-28, 1 eval at 02-16; pay 300 in P2 -> cumulative paid=300 >= 250 -> not trigger
    # Run COB first so period 2 is generated, then pay 300 in period 2
    When Admin sets the business date to "05 February 2026"
    And Admin runs inline COB job for Working Capital Loan by loanId
    And Customer makes repayment on "05 February 2026" with 300.0 transaction amount on Working Capital loan
    When Admin sets the business date to "01 March 2026"
    And Admin runs inline COB job for Working Capital Loan by loanId
    Then Working Capital loan breach schedule has the following data:
      | periodNumber | fromDate   | toDate     | minPaymentAmount | outstandingAmount | nearBreach | breach |
      | 1            | 2026-01-01 | 2026-01-31 | 500.00           | 500.00            | true       | true   |
      | 2            | 2026-02-01 | 2026-02-28 | 500.00           | 200.00            | false      | true   |
      | 3            | 2026-03-01 | 2026-03-31 | 500.00           | 500.00            | null       | null   |

  @TestRailId:C76644
  Scenario: Verify near breach with non-zero grace days shifts breach period and eval dates
    When Admin sets the business date to "01 January 2026"
    And Admin creates a client with random data
    And Admin creates a Working Capital Loan Product with breach and near breach config and overrides enabled:
      | breachFrequency | breachFrequencyType | breachAmountCalculationType | breachAmount | nearBreachFrequency | nearBreachFrequencyType | nearBreachThreshold | delinquencyGraceDays |
      | 3               | MONTHS              | FLAT                        | 900          | 60                  | DAYS                    | 33.33               | 10                   |
    And Admin creates a working capital loan using created product with the following data:
      | submittedOnDate | expectedDisbursementDate | principalAmount | totalPayment | periodPaymentRate | discount |
      | 01 January 2026 | 01 January 2026          | 9000            | 100000       | 18                | 0        |
    And Admin successfully approves the working capital loan on "01 January 2026" with "9000" amount and expected disbursement date on "01 January 2026"
    When Admin successfully disburse the Working Capital loan on "01 January 2026" with "9000" EUR transaction amount
    And Admin runs inline COB job for Working Capital Loan by loanId
    # graceDays=10 -> Period 1: 01-11 -> 04-10; freq=60d -> 1 eval at 03-12 (cumulative required = 33.33% of 900 = 299.97)
    # No payment by 03-12 -> cumulative paid=0 < 299.97 -> trigger Y
    When Admin sets the business date to "13 March 2026"
    And Admin runs inline COB job for Working Capital Loan by loanId
    Then Working Capital loan breach schedule has the following data:
      | periodNumber | fromDate   | toDate     | minPaymentAmount | outstandingAmount | nearBreach | breach |
      | 1            | 2026-01-11 | 2026-04-10 | 900.00           | 900.00            | true       | null   |

  @TestRailId:C76645
  Scenario: Verify near breach with PERCENTAGE breach amount and non-zero discount
    When Admin sets the business date to "01 January 2026"
    And Admin creates a client with random data
    And Admin creates a Working Capital Loan Product with breach and near breach config and overrides enabled:
      | breachFrequency | breachFrequencyType | breachAmountCalculationType | breachAmount | nearBreachFrequency | nearBreachFrequencyType | nearBreachThreshold | delinquencyGraceDays |
      | 2               | MONTHS              | PERCENTAGE                  | 10           | 30                  | DAYS                    | 50                  |                      |
    And Admin creates a working capital loan using created product with the following data:
      | submittedOnDate | expectedDisbursementDate | principalAmount | totalPayment | periodPaymentRate | discount |
      | 01 January 2026 | 01 January 2026          | 9000            | 100000       | 18                | 500      |
    And Admin successfully approves the working capital loan on "01 January 2026" with "9000" amount and "500" discount amount and expected disbursement date on "01 January 2026"
    When Admin successfully disburse the Working Capital loan on "01 January 2026" with "9000" EUR transaction amount and "500" discount amount
    And Admin runs inline COB job for Working Capital Loan by loanId
    # minPayment = 10% of (9000 + 500 discount) = 950; freq=30d -> 1 eval at 01-31 (cumulative required = 50% of 950 = 475)
    # No payment by 01-31 -> cumulative paid=0 < 475 -> trigger Y
    When Admin sets the business date to "01 February 2026"
    And Admin runs inline COB job for Working Capital Loan by loanId
    Then Working Capital loan breach schedule has the following data:
      | periodNumber | fromDate   | toDate     | minPaymentAmount | outstandingAmount | nearBreach | breach |
      | 1            | 2026-01-01 | 2026-02-28 | 950.00           | 950.00            | true       | null   |

  @TestRailId:C76646
  Scenario: Verify near breach stays null when eval date falls outside period due to February short month
    # Near breach freq=29 DAYS passes validation vs 1 MONTH (29 < 30 in comparator)
    # But in February (28 days), eval date = Feb 1 + 29 = Mar 2 which is outside the period
    When Admin sets the business date to "01 February 2026"
    And Admin creates a client with random data
    And Admin creates a Working Capital Loan Product with breach and near breach config and overrides enabled:
      | breachFrequency | breachFrequencyType | breachAmountCalculationType | breachAmount | nearBreachFrequency | nearBreachFrequencyType | nearBreachThreshold | delinquencyGraceDays |
      | 1               | MONTHS              | FLAT                        | 500          | 29                  | DAYS                    | 50                  |                      |
    And Admin creates a working capital loan using created product with the following data:
      | submittedOnDate  | expectedDisbursementDate | principalAmount | totalPayment | periodPaymentRate | discount |
      | 01 February 2026 | 01 February 2026         | 9000            | 100000       | 18                | 0        |
    And Admin successfully approves the working capital loan on "01 February 2026" with "9000" amount and expected disbursement date on "01 February 2026"
    When Admin successfully disburse the Working Capital loan on "01 February 2026" with "9000" EUR transaction amount
    And Admin runs inline COB job for Working Capital Loan by loanId
    When Admin sets the business date to "01 April 2026"
    And Admin runs inline COB job for Working Capital Loan by loanId
    Then Working Capital loan breach schedule has the following data:
      | periodNumber | fromDate   | toDate     | minPaymentAmount | outstandingAmount | nearBreach | breach |
      | 1            | 2026-02-01 | 2026-02-28 | 500.00           | 500.00            | null       | true   |
      | 2            | 2026-03-01 | 2026-03-31 | 500.00           | 500.00            | true       | true   |
      | 3            | 2026-04-01 | 2026-04-30 | 500.00           | 500.00            | null       | null   |

  @TestRailId:C76647
  Scenario: Verify near breach is not evaluated when eval date falls exactly on breach due date
    When Admin sets the business date to "01 January 2026"
    And Admin creates a client with random data
    And Admin creates a Working Capital Loan Product with breach and near breach config and overrides enabled:
      | breachFrequency | breachFrequencyType | breachAmountCalculationType | breachAmount | nearBreachFrequency | nearBreachFrequencyType | nearBreachThreshold | delinquencyGraceDays |
      | 2               | MONTHS              | FLAT                        | 500          | 58                  | DAYS                    | 50                  |                      |
    And Admin creates a working capital loan using created product with the following data:
      | submittedOnDate | expectedDisbursementDate | principalAmount | totalPayment | periodPaymentRate | discount |
      | 01 January 2026 | 01 January 2026          | 9000            | 100000       | 18                | 0        |
    And Admin successfully approves the working capital loan on "01 January 2026" with "9000" amount and expected disbursement date on "01 January 2026"
    When Admin successfully disburse the Working Capital loan on "01 January 2026" with "9000" EUR transaction amount
    And Admin runs inline COB job for Working Capital Loan by loanId
    # P1: 01-01 -> 02-28 (2 months). freq=58d -> candidate eval at 02-28 == toDate (breach due date).
    # Per spec: no near-breach evaluation on breach due date -> eval excluded. Period has zero applicable eval points.
    # Close-out at period end: no near-breach detected -> nearBreach=false (last-value contract; never null after period end).
    # Breach evaluation at period end: outstanding=500>0 -> breach=true.
    When Admin sets the business date to "01 March 2026"
    And Admin runs inline COB job for Working Capital Loan by loanId
    Then Working Capital loan breach schedule has the following data:
      | periodNumber | fromDate   | toDate     | minPaymentAmount | outstandingAmount | nearBreach | breach |
      | 1            | 2026-01-01 | 2026-02-28 | 500.00           | 500.00            | false      | true   |
      | 2            | 2026-03-01 | 2026-04-30 | 500.00           | 500.00            | null       | null   |

  @TestRailId:C76648
  Scenario: Verify near breach not triggered with multiple partial payments bringing outstanding below threshold
    When Admin sets the business date to "01 January 2026"
    And Admin creates a client with random data
    And Admin creates a Working Capital Loan Product with breach and near breach config and overrides enabled:
      | breachFrequency | breachFrequencyType | breachAmountCalculationType | breachAmount | nearBreachFrequency | nearBreachFrequencyType | nearBreachThreshold | delinquencyGraceDays |
      | 3               | MONTHS              | FLAT                        | 900          | 60                  | DAYS                    | 50                  |                      |
    And Admin creates a working capital loan using created product with the following data:
      | submittedOnDate | expectedDisbursementDate | principalAmount | totalPayment | periodPaymentRate | discount |
      | 01 January 2026 | 01 January 2026          | 9000            | 100000       | 18                | 0        |
    And Admin successfully approves the working capital loan on "01 January 2026" with "9000" amount and expected disbursement date on "01 January 2026"
    When Admin successfully disburse the Working Capital loan on "01 January 2026" with "9000" EUR transaction amount
    And Admin runs inline COB job for Working Capital Loan by loanId
    # freq=60d -> 1 eval at 03-02 (cumulative required = 50% of 900 = 450)
    # 3 payments by 03-02: 200(10 Jan) + 150(25 Jan) + 200(15 Feb) = 550 >= 450 -> not trigger
    # After period end -> nearBreach=false; outstanding=350>0 -> breach=true
    When Admin sets the business date to "10 January 2026"
    And Customer makes repayment on "10 January 2026" with 200.0 transaction amount on Working Capital loan
    When Admin sets the business date to "25 January 2026"
    And Customer makes repayment on "25 January 2026" with 150.0 transaction amount on Working Capital loan
    When Admin sets the business date to "15 February 2026"
    And Customer makes repayment on "15 February 2026" with 200.0 transaction amount on Working Capital loan
    When Admin sets the business date to "01 April 2026"
    And Admin runs inline COB job for Working Capital Loan by loanId
    Then Working Capital loan breach schedule has the following data:
      | periodNumber | fromDate   | toDate     | minPaymentAmount | outstandingAmount | nearBreach | breach |
      | 1            | 2026-01-01 | 2026-03-31 | 900.00           | 350.00            | false      | true   |
      | 2            | 2026-04-01 | 2026-06-30 | 900.00           | 900.00            | null       | null   |

  @TestRailId:C76649
  Scenario: Verify near breach false and breach false when full payment made before first eval date
    When Admin sets the business date to "01 January 2026"
    And Admin creates a client with random data
    And Admin creates a Working Capital Loan Product with breach and near breach config and overrides enabled:
      | breachFrequency | breachFrequencyType | breachAmountCalculationType | breachAmount | nearBreachFrequency | nearBreachFrequencyType | nearBreachThreshold | delinquencyGraceDays |
      | 3               | MONTHS              | FLAT                        | 900          | 60                  | DAYS                    | 33.33               |                      |
    And Admin creates a working capital loan using created product with the following data:
      | submittedOnDate | expectedDisbursementDate | principalAmount | totalPayment | periodPaymentRate | discount |
      | 01 January 2026 | 01 January 2026          | 9000            | 100000       | 18                | 0        |
    And Admin successfully approves the working capital loan on "01 January 2026" with "9000" amount and expected disbursement date on "01 January 2026"
    When Admin successfully disburse the Working Capital loan on "01 January 2026" with "9000" EUR transaction amount
    And Admin runs inline COB job for Working Capital Loan by loanId
    # freq=60d -> 1 eval at 03-02 (cumulative required = 33.33% of 900 = 299.97)
    # Pay 900 on 15 Jan (full) -> cumulative paid by 03-02 = 900 >= 299.97 -> not trigger
    # After period end -> nearBreach=false; outstanding=0 -> breach=false (immediate via applyRepayment)
    When Admin sets the business date to "15 January 2026"
    And Customer makes repayment on "15 January 2026" with 900.0 transaction amount on Working Capital loan
    When Admin sets the business date to "01 April 2026"
    And Admin runs inline COB job for Working Capital Loan by loanId
    Then Working Capital loan breach schedule has the following data:
      | periodNumber | fromDate   | toDate     | minPaymentAmount | outstandingAmount | nearBreach | breach |
      | 1            | 2026-01-01 | 2026-03-31 | 900.00           | 0.00              | false      | false  |
      | 2            | 2026-04-01 | 2026-06-30 | 900.00           | 900.00            | null       | null   |

  @TestRailId:C76650
  Scenario: Verify near breach evaluated correctly across 4 consecutive breach periods with mixed results
    When Admin sets the business date to "01 January 2026"
    And Admin creates a client with random data
    And Admin creates a Working Capital Loan Product with breach and near breach config and overrides enabled:
      | breachFrequency | breachFrequencyType | breachAmountCalculationType | breachAmount | nearBreachFrequency | nearBreachFrequencyType | nearBreachThreshold | delinquencyGraceDays |
      | 1               | MONTHS              | FLAT                        | 300          | 15                  | DAYS                    | 50                  |                      |
    And Admin creates a working capital loan using created product with the following data:
      | submittedOnDate | expectedDisbursementDate | principalAmount | totalPayment | periodPaymentRate | discount |
      | 01 January 2026 | 01 January 2026          | 9000            | 100000       | 18                | 0        |
    And Admin successfully approves the working capital loan on "01 January 2026" with "9000" amount and expected disbursement date on "01 January 2026"
    When Admin successfully disburse the Working Capital loan on "01 January 2026" with "9000" EUR transaction amount
    And Admin runs inline COB job for Working Capital Loan by loanId
    When Admin sets the business date to "01 February 2026"
    And Admin runs inline COB job for Working Capital Loan by loanId
    Then Working Capital loan breach schedule has the following data:
      | periodNumber | fromDate   | toDate     | minPaymentAmount | outstandingAmount | nearBreach | breach |
      | 1            | 2026-01-01 | 2026-01-31 | 300.00           | 300.00            | true       | true   |
      | 2            | 2026-02-01 | 2026-02-28 | 300.00           | 300.00            | null       | null   |
    # --- P2: pay 200 -> cumulative paid by eval#1 (02-16) = 200 >= 150 -> nearBreach=false; outstanding=100>0 -> breach=true ---
    When Admin sets the business date to "05 February 2026"
    And Customer makes repayment on "05 February 2026" with 200.0 transaction amount on Working Capital loan
    When Admin sets the business date to "01 March 2026"
    And Admin runs inline COB job for Working Capital Loan by loanId
    Then Working Capital loan breach schedule has the following data:
      | periodNumber | fromDate   | toDate     | minPaymentAmount | outstandingAmount | nearBreach | breach |
      | 1            | 2026-01-01 | 2026-01-31 | 300.00           | 300.00            | true       | true   |
      | 2            | 2026-02-01 | 2026-02-28 | 300.00           | 100.00            | false      | true   |
      | 3            | 2026-03-01 | 2026-03-31 | 300.00           | 300.00            | null       | null   |
    # --- P3: no payment by eval#1 (03-16) -> cumulative paid=0 < 150 -> nearBreach=true; breach=true ---
    When Admin sets the business date to "01 April 2026"
    And Admin runs inline COB job for Working Capital Loan by loanId
    # --- P4: pay 300 on 04-01 -> cumulative paid by eval#1 (04-16) = 300 >= 150 -> nearBreach=false; outstanding=0 -> breach=false (immediate via applyRepayment) ---
    And Customer makes repayment on "01 April 2026" with 300.0 transaction amount on Working Capital loan
    When Admin sets the business date to "01 May 2026"
    And Admin runs inline COB job for Working Capital Loan by loanId
    Then Working Capital loan breach schedule has the following data:
      | periodNumber | fromDate   | toDate     | minPaymentAmount | outstandingAmount | nearBreach | breach |
      | 1            | 2026-01-01 | 2026-01-31 | 300.00           | 300.00            | true       | true   |
      | 2            | 2026-02-01 | 2026-02-28 | 300.00           | 100.00            | false      | true   |
      | 3            | 2026-03-01 | 2026-03-31 | 300.00           | 300.00            | true       | true   |
      | 4            | 2026-04-01 | 2026-04-30 | 300.00           | 0.00              | false      | false  |
      | 5            | 2026-05-01 | 2026-05-31 | 300.00           | 300.00            | null       | null   |

  @TestRailId:C76651
  Scenario: Verify non-disbursed loan has no breach schedule and no near breach evaluation
    When Admin sets the business date to "01 January 2026"
    And Admin creates a client with random data
    And Admin creates a Working Capital Loan Product with breach and near breach config and overrides enabled:
      | breachFrequency | breachFrequencyType | breachAmountCalculationType | breachAmount | nearBreachFrequency | nearBreachFrequencyType | nearBreachThreshold | delinquencyGraceDays |
      | 3               | MONTHS              | FLAT                        | 900          | 60                  | DAYS                    | 33.33               |                      |
    And Admin creates a working capital loan using created product with the following data:
      | submittedOnDate | expectedDisbursementDate | principalAmount | totalPayment | periodPaymentRate | discount |
      | 01 January 2026 | 01 January 2026          | 9000            | 100000       | 18                | 0        |
    And Admin successfully approves the working capital loan on "01 January 2026" with "9000" amount and expected disbursement date on "01 January 2026"
    # Loan is approved but NOT disbursed - no breach schedule should exist
    Then Working Capital loan breach schedule has no data

  @TestRailId:C80947
  Scenario: Verify near breach eval#1 OK, eval#2 fails with cumulative stepped threshold
    When Admin sets the business date to "01 January 2026"
    And Admin creates a client with random data
    And Admin creates a Working Capital Loan Product with breach and near breach config and overrides enabled:
      | breachFrequency | breachFrequencyType | breachAmountCalculationType | breachAmount | nearBreachFrequency | nearBreachFrequencyType | nearBreachThreshold | delinquencyGraceDays |
      | 9               | DAYS                | FLAT                        | 90           | 3                   | DAYS                    | 33                  |                      |
    And Admin creates a working capital loan using created product with the following data:
      | submittedOnDate | expectedDisbursementDate | principalAmount | totalPayment | periodPaymentRate | discount |
      | 01 January 2026 | 01 January 2026          | 9000            | 100000       | 18                | 0        |
    And Admin successfully approves the working capital loan on "01 January 2026" with "9000" amount and expected disbursement date on "01 January 2026"
    When Admin successfully disburse the Working Capital loan on "01 January 2026" with "9000" EUR transaction amount
    And Admin runs inline COB job for Working Capital Loan by loanId
    # P1: 01-01 -> 01-09 (9d), freq=3d -> 2 evals: 01-04, 01-07; step required = 33% of 90 = 29.7, cumulative required = N * 29.7.
    # Pay 30 on 02 Jan: cumulative paid by eval#1 (01-04) = 30 >= 1 * 29.7 -> not trigger.
    # Pay 10 on 05 Jan: cumulative paid by eval#2 (01-07) = 40 < 2 * 29.7 = 59.4 -> trigger Y at eval#2.
    When Admin sets the business date to "02 January 2026"
    And Customer makes repayment on "02 January 2026" with 30.0 transaction amount on Working Capital loan
    When Admin sets the business date to "05 January 2026"
    And Customer makes repayment on "05 January 2026" with 10.0 transaction amount on Working Capital loan
    When Admin sets the business date to "08 January 2026"
    And Admin runs inline COB job for Working Capital Loan by loanId
    Then Working Capital loan breach schedule has the following data:
      | periodNumber | fromDate   | toDate     | minPaymentAmount | outstandingAmount | nearBreach | breach |
      | 1            | 2026-01-01 | 2026-01-09 | 90.00            | 50.00             | true       | null   |

  @TestRailId:C80948
  Scenario: Verify near breach not triggered when full minimum is paid front-loaded - cumulative requirement satisfied at every eval
    When Admin sets the business date to "01 January 2026"
    And Admin creates a client with random data
    And Admin creates a Working Capital Loan Product with breach and near breach config and overrides enabled:
      | breachFrequency | breachFrequencyType | breachAmountCalculationType | breachAmount | nearBreachFrequency | nearBreachFrequencyType | nearBreachThreshold | delinquencyGraceDays |
      | 9               | DAYS                | FLAT                        | 90           | 3                   | DAYS                    | 50                  |                      |
    And Admin creates a working capital loan using created product with the following data:
      | submittedOnDate | expectedDisbursementDate | principalAmount | totalPayment | periodPaymentRate | discount |
      | 01 January 2026 | 01 January 2026          | 9000            | 100000       | 18                | 0        |
    And Admin successfully approves the working capital loan on "01 January 2026" with "9000" amount and expected disbursement date on "01 January 2026"
    When Admin successfully disburse the Working Capital loan on "01 January 2026" with "9000" EUR transaction amount
    And Admin runs inline COB job for Working Capital Loan by loanId
    # P1: 01-01 -> 01-09 (9d), freq=3d -> 2 evals: 01-04, 01-07; step required = 50% of 90 = 45, cumulative required = N * 45.
    # Pay 90 on 02 Jan (full minimum upfront).
    # Eval#1 (01-04): cumulative paid = 90 >= 1 * 45 -> not trigger.
    # Eval#2 (01-07): cumulative paid = 90 vs 2 * 45 = 90; strict less-than (90 < 90 is false) -> not trigger.
    # After period end (01-10): all evals passed without trigger -> close-out sets nearBreach=false; outstanding=0 -> breach=false.
    When Admin sets the business date to "02 January 2026"
    And Customer makes repayment on "02 January 2026" with 90.0 transaction amount on Working Capital loan
    When Admin sets the business date to "10 January 2026"
    And Admin runs inline COB job for Working Capital Loan by loanId
    Then Working Capital loan breach schedule has the following data:
      | periodNumber | fromDate   | toDate     | minPaymentAmount | outstandingAmount | nearBreach | breach |
      | 1            | 2026-01-01 | 2026-01-09 | 90.00            | 0.00              | false      | false  |
      | 2            | 2026-01-10 | 2026-01-18 | 90.00            | 90.00             | null       | null   |

  @TestRailId:C80949
  Scenario: Verify near breach false when cumulative payments meet stepped requirements across multi-eval period
    When Admin sets the business date to "01 January 2026"
    And Admin creates a client with random data
    And Admin creates a Working Capital Loan Product with breach and near breach config and overrides enabled:
      | breachFrequency | breachFrequencyType | breachAmountCalculationType | breachAmount | nearBreachFrequency | nearBreachFrequencyType | nearBreachThreshold | delinquencyGraceDays |
      | 9               | DAYS                | FLAT                        | 90           | 3                   | DAYS                    | 33                  |                      |
    And Admin creates a working capital loan using created product with the following data:
      | submittedOnDate | expectedDisbursementDate | principalAmount | totalPayment | periodPaymentRate | discount |
      | 01 January 2026 | 01 January 2026          | 9000            | 100000       | 18                | 0        |
    And Admin successfully approves the working capital loan on "01 January 2026" with "9000" amount and expected disbursement date on "01 January 2026"
    When Admin successfully disburse the Working Capital loan on "01 January 2026" with "9000" EUR transaction amount
    And Admin runs inline COB job for Working Capital Loan by loanId
    # P1: 01-01 -> 01-09 (9d), evals at 01-04, 01-07; step required = 33% of 90 = 29.7, cumulative required = N * 29.7.
    # Pay 30 on 02 Jan: cumulative paid by eval#1 (01-04) = 30 >= 29.7 -> not trigger.
    # Pay 30 on 05 Jan: cumulative paid by eval#2 (01-07) = 60 >= 59.4 -> not trigger.
    # After period end (01-10) -> close-out sets nearBreach=false; outstanding=30>0 -> breach=true.
    When Admin sets the business date to "02 January 2026"
    And Customer makes repayment on "02 January 2026" with 30.0 transaction amount on Working Capital loan
    When Admin sets the business date to "05 January 2026"
    And Customer makes repayment on "05 January 2026" with 30.0 transaction amount on Working Capital loan
    When Admin sets the business date to "10 January 2026"
    And Admin runs inline COB job for Working Capital Loan by loanId
    Then Working Capital loan breach schedule has the following data:
      | periodNumber | fromDate   | toDate     | minPaymentAmount | outstandingAmount | nearBreach | breach |
      | 1            | 2026-01-01 | 2026-01-09 | 90.00            | 30.00             | false      | true   |
      | 2            | 2026-01-10 | 2026-01-18 | 90.00            | 90.00             | null       | null   |

  @TestRailId:C80950
  Scenario: Verify that near breach is detected by cumulative paid falling below stepped requirement across two consecutive breach periods - UC1
    When Admin sets the business date to "01 January 2026"
    And Admin creates a client with random data
    And Admin creates a Working Capital Loan Product with breach and near breach config and overrides enabled:
      | breachFrequency | breachFrequencyType | breachAmountCalculationType | breachAmount | nearBreachFrequency | nearBreachFrequencyType | nearBreachThreshold | delinquencyGraceDays |
      | 9               | DAYS                | PERCENTAGE                  | 50           | 3                   | DAYS                    | 33                  |                      |
    And Admin creates a working capital loan using created product with the following data:
      | submittedOnDate | expectedDisbursementDate | principalAmount | totalPayment | periodPaymentRate | discount |
      | 01 January 2026 | 01 January 2026          | 800             | 100000       | 18                | 0        |
    And Admin successfully approves the working capital loan on "01 January 2026" with "800" amount and expected disbursement date on "01 January 2026"
    When Admin successfully disburse the Working Capital loan on "01 January 2026" with "800" EUR transaction amount
    And Admin runs inline COB job for Working Capital Loan by loanId
    # P1: 01-01 -> 01-09 (9d), freq=3d -> 2 evals: 01-04, 01-07; minPayment = 50% of 800 = 400; step required = 33% of 400 = 132, cumulative required = N * 132.
    # Pay 200 on 02 Jan: cumulative paid by eval#1 (01-04) = 200 >= 132 -> not trigger.
    # Pay 50 on 05 Jan: cumulative paid by eval#2 (01-07) = 250 < 264 -> trigger Y at eval#2.
    When Admin sets the business date to "02 January 2026"
    And Customer makes repayment on "02 January 2026" with 200.0 transaction amount on Working Capital loan
    When Admin sets the business date to "05 January 2026"
    And Customer makes repayment on "05 January 2026" with 50.0 transaction amount on Working Capital loan
    When Admin sets the business date to "08 January 2026"
    And Admin runs inline COB job for Working Capital Loan by loanId
    Then Working Capital loan breach schedule has the following data:
      | periodNumber | fromDate   | toDate     | minPaymentAmount | outstandingAmount | nearBreach | breach |
      | 1            | 2026-01-01 | 2026-01-09 | 400.00           | 150.00            | true       | null   |
    # Advance past P1 end (01-09) and P2 eval#1 (01-13).
    # P1 breach: paid=250 < min=400 -> breach=true. P1 nearBreach remains true (immutable).
    # P2: 01-10 -> 01-18, 2 evals: 01-13, 01-16. Step required = 132.
    # No payment in P2 -> cumulative at eval#1 (01-13) = 0 < 132 -> trigger Y.
    When Admin sets the business date to "14 January 2026"
    And Admin runs inline COB job for Working Capital Loan by loanId
    Then Working Capital loan breach schedule has the following data:
      | periodNumber | fromDate   | toDate     | minPaymentAmount | outstandingAmount | nearBreach | breach |
      | 1            | 2026-01-01 | 2026-01-09 | 400.00           | 150.00            | true       | true   |
      | 2            | 2026-01-10 | 2026-01-18 | 400.00           | 400.00            | true       | null   |

  @TestRailId:C80951
  Scenario: Verify that near breach evaluation is idempotent across multiple COB runs on the same business date - UC2
    When Admin sets the business date to "01 January 2026"
    And Admin creates a client with random data
    And Admin creates a Working Capital Loan Product with breach and near breach config and overrides enabled:
      | breachFrequency | breachFrequencyType | breachAmountCalculationType | breachAmount | nearBreachFrequency | nearBreachFrequencyType | nearBreachThreshold | delinquencyGraceDays |
      | 9               | DAYS                | FLAT                        | 90           | 3                   | DAYS                    | 33.33               |                      |
    And Admin creates a working capital loan using created product with the following data:
      | submittedOnDate | expectedDisbursementDate | principalAmount | totalPayment | periodPaymentRate | discount |
      | 01 January 2026 | 01 January 2026          | 9000            | 100000       | 18                | 0        |
    And Admin successfully approves the working capital loan on "01 January 2026" with "9000" amount and expected disbursement date on "01 January 2026"
    When Admin successfully disburse the Working Capital loan on "01 January 2026" with "9000" EUR transaction amount
    And Admin runs inline COB job for Working Capital Loan by loanId
    # P1: 01-01 -> 01-09, freq=3d -> 2 evals: 01-04, 01-07. Step required = 33.33% of 90 = 29.997.
    # No payment -> cum at eval#1 (01-04) = 0 < 29.997 -> trigger Y at eval#1.
    When Admin sets the business date to "05 January 2026"
    And Admin runs inline COB job for Working Capital Loan by loanId
    Then Working Capital loan breach schedule has the following data:
      | periodNumber | fromDate   | toDate     | minPaymentAmount | outstandingAmount | nearBreach | breach |
      | 1            | 2026-01-01 | 2026-01-09 | 90.00            | 90.00             | true       | null   |
    # Re-run COB on the same business date. State must remain unchanged (immutability gate via nearBreach != null).
    And Admin runs inline COB job for Working Capital Loan by loanId
    Then Working Capital loan breach schedule has the following data:
      | periodNumber | fromDate   | toDate     | minPaymentAmount | outstandingAmount | nearBreach | breach |
      | 1            | 2026-01-01 | 2026-01-09 | 90.00            | 90.00             | true       | null   |

  @TestRailId:C80952
  Scenario: Verify that near breach stays immutable when backdated repayment with transaction date inside window is posted later - UC3
    When Admin sets the business date to "01 January 2026"
    And Admin creates a client with random data
    And Admin creates a Working Capital Loan Product with breach and near breach config and overrides enabled:
      | breachFrequency | breachFrequencyType | breachAmountCalculationType | breachAmount | nearBreachFrequency | nearBreachFrequencyType | nearBreachThreshold | delinquencyGraceDays |
      | 9               | DAYS                | FLAT                        | 90           | 3                   | DAYS                    | 33.33               |                      |
    And Admin creates a working capital loan using created product with the following data:
      | submittedOnDate | expectedDisbursementDate | principalAmount | totalPayment | periodPaymentRate | discount |
      | 01 January 2026 | 01 January 2026          | 9000            | 100000       | 18                | 0        |
    And Admin successfully approves the working capital loan on "01 January 2026" with "9000" amount and expected disbursement date on "01 January 2026"
    When Admin successfully disburse the Working Capital loan on "01 January 2026" with "9000" EUR transaction amount
    And Admin runs inline COB job for Working Capital Loan by loanId
    # P1: 01-01 -> 01-09, eval#1 at 01-04 (cumulative required = 29.997). No payment -> trigger Y at eval#1.
    When Admin sets the business date to "05 January 2026"
    And Admin runs inline COB job for Working Capital Loan by loanId
    Then Working Capital loan breach schedule has the following data:
      | periodNumber | fromDate   | toDate     | minPaymentAmount | outstandingAmount | nearBreach | breach |
      | 1            | 2026-01-01 | 2026-01-09 | 90.00            | 90.00             | true       | null   |
    # Now post a backdated repayment of 50 dated 02 Jan (before eval#1).
    # If re-evaluated, paid 50 >= 29.997 would NOT trigger. But nearBreach is immutable -> stays true.
    # paidAmount/outstanding update synchronously via applyRepayment.
    When Admin sets the business date to "06 January 2026"
    And Customer makes repayment on "02 January 2026" with 50.0 transaction amount on Working Capital loan
    And Admin runs inline COB job for Working Capital Loan by loanId
    Then Working Capital loan breach schedule has the following data:
      | periodNumber | fromDate   | toDate     | minPaymentAmount | outstandingAmount | nearBreach | breach |
      | 1            | 2026-01-01 | 2026-01-09 | 90.00            | 40.00             | true       | null   |

  @TestRailId:C80953
  Scenario: Verify that grace days shift breach period start and near breach is evaluated at shifted eval dates - UC4
    When Admin sets the business date to "01 January 2026"
    And Admin creates a client with random data
    And Admin creates a Working Capital Loan Product with breach and near breach config and overrides enabled:
      | breachFrequency | breachFrequencyType | breachAmountCalculationType | breachAmount | nearBreachFrequency | nearBreachFrequencyType | nearBreachThreshold | delinquencyGraceDays |
      | 9               | DAYS                | PERCENTAGE                  | 50           | 3                   | DAYS                    | 33                  | 3                    |
    And Admin creates a working capital loan using created product with the following data:
      | submittedOnDate | expectedDisbursementDate | principalAmount | totalPayment | periodPaymentRate | discount |
      | 01 January 2026 | 01 January 2026          | 800             | 100000       | 18                | 0        |
    And Admin successfully approves the working capital loan on "01 January 2026" with "800" amount and expected disbursement date on "01 January 2026"
    When Admin successfully disburse the Working Capital loan on "01 January 2026" with "800" EUR transaction amount
    And Admin runs inline COB job for Working Capital Loan by loanId
    # Grace=3 -> P1: 01-04 -> 01-12 (9d from 01-04 minus 1 day). minPayment = 50% of 800 = 400.
    # near-breach freq=3 -> evals: 01-07 (#1), 01-10 (#2). step required = 33% of 400 = 132.
    # Pay 100 on 05 Jan -> cumulative paid by eval#1 is 100.
    When Admin sets the business date to "05 January 2026"
    And Customer makes repayment on "05 January 2026" with 100.0 transaction amount on Working Capital loan
    # Phase A: current date 06 Jan (BEFORE eval#1 at 01-07) -> nearBreach=null.
    When Admin sets the business date to "06 January 2026"
    And Admin runs inline COB job for Working Capital Loan by loanId
    Then Working Capital loan breach schedule has the following data:
      | periodNumber | fromDate   | toDate     | minPaymentAmount | outstandingAmount | nearBreach | breach |
      | 1            | 2026-01-04 | 2026-01-12 | 400.00           | 300.00            | null       | null   |
    # Phase B: advance past eval#1 (01-07) -> cumulative paid by 01-07 = 100 < 132 -> trigger Y at eval#1.
    When Admin sets the business date to "08 January 2026"
    And Admin runs inline COB job for Working Capital Loan by loanId
    Then Working Capital loan breach schedule has the following data:
      | periodNumber | fromDate   | toDate     | minPaymentAmount | outstandingAmount | nearBreach | breach |
      | 1            | 2026-01-04 | 2026-01-12 | 400.00           | 300.00            | true       | null   |

  @TestRailId:C80954
  Scenario: Verify that near breach stays null between evals and is detected when cumulative paid falls short of stepped requirement at a later eval - UC5
    When Admin sets the business date to "01 January 2026"
    And Admin creates a client with random data
    And Admin creates a Working Capital Loan Product with breach and near breach config and overrides enabled:
      | breachFrequency | breachFrequencyType | breachAmountCalculationType | breachAmount | nearBreachFrequency | nearBreachFrequencyType | nearBreachThreshold | delinquencyGraceDays |
      | 9               | DAYS                | PERCENTAGE                  | 10           | 3                   | DAYS                    | 33                  |                      |
    And Admin creates a working capital loan using created product with the following data:
      | submittedOnDate | expectedDisbursementDate | principalAmount | totalPayment | periodPaymentRate | discount |
      | 01 January 2026 | 01 January 2026          | 10000           | 100000       | 18                | 0        |
    And Admin successfully approves the working capital loan on "01 January 2026" with "10000" amount and expected disbursement date on "01 January 2026"
    When Admin successfully disburse the Working Capital loan on "01 January 2026" with "10000" EUR transaction amount
    And Admin runs inline COB job for Working Capital Loan by loanId
    # P1: 01-01 -> 01-09 (9d). minPayment = 10% of 10000 = 1000.
    # near-breach freq=3 -> eval points: 01-04 (#1), 01-07 (#2); 01-10 lands on/after toDate (01-09) and is excluded.
    # Step required = 33% of 1000 = 330. Cumulative required at eval#N = N * 330 (330, 660).
    # Pay 400 on 02 Jan -> cumulative at 01-04 = 400 >= 330 -> not trigger.
    # No further payment -> cumulative at 01-07 = 400 < 660 -> trigger Y at eval#2.
    When Admin sets the business date to "02 January 2026"
    And Customer makes repayment on "02 January 2026" with 400.0 transaction amount on Working Capital loan
    # Phase A: COB on 05 Jan (AFTER eval#1, BEFORE eval#2) -> #1 passed without trigger, #2 not yet evaluated, period not ended -> nearBreach stays null.
    When Admin sets the business date to "05 January 2026"
    And Admin runs inline COB job for Working Capital Loan by loanId
    Then Working Capital loan breach schedule has the following data:
      | periodNumber | fromDate   | toDate     | minPaymentAmount | outstandingAmount | nearBreach | breach |
      | 1            | 2026-01-01 | 2026-01-09 | 1000.00          | 600.00            | null       | null   |
    # Phase B: COB on 08 Jan (AFTER eval#2) -> eval#2 triggers (400 < 660) -> nearBreach=true.
    When Admin sets the business date to "08 January 2026"
    And Admin runs inline COB job for Working Capital Loan by loanId
    Then Working Capital loan breach schedule has the following data:
      | periodNumber | fromDate   | toDate     | minPaymentAmount | outstandingAmount | nearBreach | breach |
      | 1            | 2026-01-01 | 2026-01-09 | 1000.00          | 600.00            | true       | null   |

  @TestRailId:C80955
  Scenario: Verify credit balance refund does not mutate already evaluated near breach schedule - UC6
    When Admin sets the business date to "01 January 2026"
    And Admin creates a client with random data
    And Admin creates a Working Capital Loan Product with breach and near breach config and overrides enabled:
      | breachFrequency | breachFrequencyType | breachAmountCalculationType | breachAmount | nearBreachFrequency | nearBreachFrequencyType | nearBreachThreshold | delinquencyGraceDays |
      | 9               | DAYS                | FLAT                        | 90           | 3                   | DAYS                    | 33.33               |                      |
    And Admin creates a working capital loan using created product with the following data:
      | submittedOnDate | expectedDisbursementDate | principalAmount | totalPayment | periodPaymentRate | discount |
      | 01 January 2026 | 01 January 2026          | 9000            | 100000       | 18                | 0        |
    And Admin successfully approves the working capital loan on "01 January 2026" with "9000" amount and expected disbursement date on "01 January 2026"
    When Admin successfully disburse the Working Capital loan on "01 January 2026" with "9000" EUR transaction amount
    And Admin runs inline COB job for Working Capital Loan by loanId
    # No payment by eval#1 (01-04) -> nearBreach=true.
    When Admin sets the business date to "05 January 2026"
    And Admin runs inline COB job for Working Capital Loan by loanId
    Then Working Capital loan breach schedule has the following data:
      | periodNumber | fromDate   | toDate     | minPaymentAmount | outstandingAmount | nearBreach | breach |
      | 1            | 2026-01-01 | 2026-01-09 | 90.00            | 90.00             | true       | null   |
    # Overpay the loan and post CBR. CBR must not clear or recalculate the already evaluated nearBreach flag.
    When Admin sets the business date to "06 January 2026"
    And Customer makes repayment on "06 January 2026" with 9500.0 transaction amount on Working Capital loan
    And Customer makes credit balance refund on "06 January 2026" with 500.0 transaction amount on Working Capital loan
    Then Working Capital loan breach schedule has the following data:
      | periodNumber | fromDate   | toDate     | minPaymentAmount | outstandingAmount | nearBreach | breach |
      | 1            | 2026-01-01 | 2026-01-09 | 90.00            | 0.00              | true       | false  |

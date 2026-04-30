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
    # Breach period 1: 01 Jan -> 31 Mar (90 days), minPayment=900
    # Near breach eval date: 01 Jan + 60 = 02 Mar 2026
    # No payment made -> outstanding% = 100% > 33.33% -> near breach
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
    # Pay 700 before evaluation date -> outstanding = 200, outstanding% = 200/900 = 22.22% < 33.33%
    When Admin sets the business date to "15 February 2026"
    And Admin makes Internal Payment "700.0" on "2026-02-15"
    # After eval date (02 Mar), outstanding% = 22.22% which is NOT > 33.33% -> no near breach
    # After breach period end (31 Mar), near breach = false
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
    # Eval date passes (02 Mar), no payment -> near breach = true
    When Admin sets the business date to "03 March 2026"
    And Admin runs inline COB job for Working Capital Loan by loanId
    Then Working Capital loan breach schedule has the following data:
      | periodNumber | fromDate   | toDate     | minPaymentAmount | outstandingAmount | nearBreach | breach |
      | 1            | 2026-01-01 | 2026-03-31 | 900.00           | 900.00            | true       | null   |
    # Now pay full amount - near breach must stay true (immutable)
    When Admin sets the business date to "15 March 2026"
    And Admin makes Internal Payment "900.0" on "2026-03-15"
    When Admin sets the business date to "01 April 2026"
    And Admin runs inline COB job for Working Capital Loan by loanId
    Then Working Capital loan breach schedule has the following data:
      | periodNumber | fromDate   | toDate     | minPaymentAmount | outstandingAmount | nearBreach | breach |
      | 1            | 2026-01-01 | 2026-03-31 | 900.00           | 0.00              | true       | false  |
      | 2            | 2026-04-01 | 2026-06-30 | 900.00           | 900.00            | null       | null   |

  @TestRailId:C76639
  Scenario: Verify near breach false when payment keeps outstanding below threshold across all eval points
    When Admin sets the business date to "01 January 2026"
    And Admin creates a client with random data
    And Admin creates a Working Capital Loan Product with breach and near breach config and overrides enabled:
      | breachFrequency | breachFrequencyType | breachAmountCalculationType | breachAmount | nearBreachFrequency | nearBreachFrequencyType | nearBreachThreshold | delinquencyGraceDays |
      | 3               | MONTHS              | FLAT                        | 900          | 30                  | DAYS                    | 50                  |                      |
    And Admin creates a working capital loan using created product with the following data:
      | submittedOnDate | expectedDisbursementDate | principalAmount | totalPayment | periodPaymentRate | discount |
      | 01 January 2026 | 01 January 2026          | 9000            | 100000       | 18                | 0        |
    And Admin successfully approves the working capital loan on "01 January 2026" with "9000" amount and expected disbursement date on "01 January 2026"
    When Admin successfully disburse the Working Capital loan on "01 January 2026" with "9000" EUR transaction amount
    And Admin runs inline COB job for Working Capital Loan by loanId
    # threshold=50%, minPayment=900
    # Pay 500 before first eval -> outstanding=400, outstanding%=44.44% < 50% -> no near breach at any eval
    When Admin sets the business date to "20 January 2026"
    And Admin makes Internal Payment "500.0" on "2026-01-20"
    When Admin sets the business date to "01 February 2026"
    And Admin runs inline COB job for Working Capital Loan by loanId
    Then Working Capital loan breach schedule has the following data:
      | periodNumber | fromDate   | toDate     | minPaymentAmount | outstandingAmount | nearBreach | breach |
      | 1            | 2026-01-01 | 2026-03-31 | 900.00           | 400.00            | null       | null   |
    # After period end: all eval points passed, none triggered -> nearBreach=false, breach=true (outstanding 400 > 0)
    When Admin sets the business date to "01 April 2026"
    And Admin runs inline COB job for Working Capital Loan by loanId
    Then Working Capital loan breach schedule has the following data:
      | periodNumber | fromDate   | toDate     | minPaymentAmount | outstandingAmount | nearBreach | breach |
      | 1            | 2026-01-01 | 2026-03-31 | 900.00           | 400.00            | false      | true   |
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
    # Eval date = 01 Jan + 60 = 02 Mar 2026. Run COB before that -> near breach stays null
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
    # minPayment = 10% of 9000 = 900. Breach period: 01 Jan -> 28 Feb (2 months - 1 day)
    # Near breach eval dates: 01 Jan + 2 weeks = 15 Jan, 29 Jan, 12 Feb, 26 Feb
    # threshold=50%, required=450. No payment -> outstanding%=100% > 50% -> near breach at first eval (15 Jan)
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
    # threshold=50%, minPayment=900 -> boundary = 450 (50% of 900)
    # Pay exactly 450 -> outstanding=450, outstanding%=50% = threshold -> NOT > threshold -> no near breach
    When Admin sets the business date to "15 January 2026"
    And Admin makes Internal Payment "450.0" on "2026-01-15"
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
    # Period 1: 01 Jan -> 31 Jan, minPayment=500, eval date=16 Jan
    # No payment in period 1 -> outstanding%=100% > 50% -> nearBreach=true
    # Period 2: 01 Feb -> 28 Feb, minPayment=500, eval date=16 Feb
    # Run COB first so period 2 is generated, then pay 300 in period 2
    When Admin sets the business date to "05 February 2026"
    And Admin runs inline COB job for Working Capital Loan by loanId
    And Admin makes Internal Payment "300.0" on "2026-02-05"
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
    When Admin successfully disburse the Working Capital loan on "01 January 2026" with "9000" EUR transaction amount
    And Admin runs inline COB job for Working Capital Loan by loanId
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
      | submittedOnDate | expectedDisbursementDate | principalAmount | totalPayment | periodPaymentRate | discount |
      | 01 February 2026 | 01 February 2026        | 9000            | 100000       | 18                | 0        |
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
  Scenario: Verify near breach eval date exactly on period end - both evaluated in same COB run
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
    When Admin sets the business date to "01 March 2026"
    And Admin runs inline COB job for Working Capital Loan by loanId
    Then Working Capital loan breach schedule has the following data:
      | periodNumber | fromDate   | toDate     | minPaymentAmount | outstandingAmount | nearBreach | breach |
      | 1            | 2026-01-01 | 2026-02-28 | 500.00           | 500.00            | true       | true   |
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
    When Admin sets the business date to "10 January 2026"
    And Admin makes Internal Payment "200.0" on "2026-01-10"
    When Admin sets the business date to "25 January 2026"
    And Admin makes Internal Payment "150.0" on "2026-01-25"
    When Admin sets the business date to "15 February 2026"
    And Admin makes Internal Payment "200.0" on "2026-02-15"
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
    When Admin sets the business date to "15 January 2026"
    And Admin makes Internal Payment "900.0" on "2026-01-15"
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
    # --- P2: pay 200, outstanding=100, 33.3% < 50% -> nearBreach=false, breach=true ---
    When Admin sets the business date to "05 February 2026"
    And Admin makes Internal Payment "200.0" on "2026-02-05"
    When Admin sets the business date to "01 March 2026"
    And Admin runs inline COB job for Working Capital Loan by loanId
    Then Working Capital loan breach schedule has the following data:
      | periodNumber | fromDate   | toDate     | minPaymentAmount | outstandingAmount | nearBreach | breach |
      | 1            | 2026-01-01 | 2026-01-31 | 300.00           | 300.00            | true       | true   |
      | 2            | 2026-02-01 | 2026-02-28 | 300.00           | 100.00            | false      | true   |
      | 3            | 2026-03-01 | 2026-03-31 | 300.00           | 300.00            | null       | null   |
    # --- P3: no payment, 100% > 50% -> nearBreach=true, breach=true ---
    When Admin sets the business date to "01 April 2026"
    And Admin runs inline COB job for Working Capital Loan by loanId
    # --- P4: pay full 300, outstanding=0 -> breach=false (immediate), nearBreach=false (after period end) ---
    And Admin makes Internal Payment "300.0" on "2026-04-01"
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

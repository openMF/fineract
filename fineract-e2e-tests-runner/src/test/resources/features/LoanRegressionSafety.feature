@RegressionSafety
Feature: Loan Lifecycle Regression Safety
  Exercises the complete loan lifecycle through all major state transitions.
  Designed to be run against multiple Fineract instances with different DB states
  to catch regressions introduced by database schema changes.
  Based on proven patterns from 0_COB.feature (C2501, C2681).

  @TestRailId:C76758
  Scenario: Verify loan delinquency lifecycle (in-advance + late repayment) after DB changes
    # --- Setup + disburse --------------------------------------------------
    When Admin sets the business date to "01 January 2022"
    When Admin creates a client with random data
    When Admin creates a new default Loan with date: "01 January 2022"
    And Admin successfully approves the loan on "01 January 2022" with "1000" amount and expected disbursement date on "01 January 2022"
    And Admin successfully disburse the loan on "01 January 2022" with "1000" EUR transaction amount
    Then Loan status will be "ACTIVE"
    Then Loan has 1000 outstanding amount
    Then Loan Repayment schedule has 1 periods, with the following data for periods:
      | Nr | Days | Date            | Paid date | Balance of loan | Principal due | Interest | Fees | Penalties | Due    | Paid | In advance | Late | Outstanding |
      |    |      | 01 January 2022 |           | 1000.0          |               |          | 0.0  |           | 0.0    | 0.0  |            |      |             |
      | 1  | 30   | 31 January 2022 |           | 0.0             | 1000.0        | 0.0      | 0.0  | 0.0       | 1000.0 | 0.0  | 0.0        | 0.0  | 1000.0      |
    Then Loan Repayment schedule has the following data in Total row:
      | Principal due | Interest | Fees | Penalties | Due    | Paid | In advance | Late | Outstanding |
      | 1000.0        | 0.0      | 0.0  | 0.0       | 1000.0 | 0.0  | 0.0        | 0.0  | 1000.0      |
    Then Loan Transactions tab has the following data:
      | Transaction date | Transaction Type | Amount | Principal | Interest | Fees | Penalties | Loan Balance |
      | 01 January 2022  | Disbursement     | 1000.0 | 0.0       | 0.0      | 0.0  | 0.0       | 1000.0       |

    # --- IN-ADVANCE partial repayment (15 Jan, before due 31 Jan) ---------
    # Writes to m_loan_transaction, m_loan_transaction_repayment_schedule_mapping
    # AND records the amount in the "In advance" column of the schedule.
    When Admin sets the business date to "15 January 2022"
    And Customer makes "AUTOPAY" repayment on "15 January 2022" with 500 EUR transaction amount
    Then Repayment transaction is created with 500 amount and "AUTOPAY" type
    Then Loan has 500 outstanding amount
    Then Loan status will be "ACTIVE"
    Then Loan Repayment schedule has 1 periods, with the following data for periods:
      | Nr | Days | Date            | Paid date | Balance of loan | Principal due | Interest | Fees | Penalties | Due    | Paid  | In advance | Late | Outstanding |
      |    |      | 01 January 2022 |           | 1000.0          |               |          | 0.0  |           | 0.0    | 0.0   |            |      |             |
      | 1  | 30   | 31 January 2022 |           | 0.0             | 1000.0        | 0.0      | 0.0  | 0.0       | 1000.0 | 500.0 | 500.0      | 0.0  | 500.0       |
    Then Loan Repayment schedule has the following data in Total row:
      | Principal due | Interest | Fees | Penalties | Due    | Paid  | In advance | Late | Outstanding |
      | 1000.0        | 0.0      | 0.0  | 0.0       | 1000.0 | 500.0 | 500.0      | 0.0  | 500.0       |
    Then Loan Transactions tab has the following data:
      | Transaction date | Transaction Type | Amount | Principal | Interest | Fees | Penalties | Loan Balance |
      | 01 January 2022  | Disbursement     | 1000.0 | 0.0       | 0.0      | 0.0  | 0.0       | 1000.0       |
      | 15 January 2022  | Repayment        | 500.0  | 500.0     | 0.0      | 0.0  | 0.0       | 500.0        |

    # --- Advance past due (04 Feb) + COB triggers delinquency --------------
    # Default LP1 has an arrears setting of 3 days -> delinquent from 03 Feb.
    # Writes to m_loan (lastClosedBusinessDate), m_loan_delinquency_tag_history,
    # m_delinquency_range. Interest-free, no charges -> no Accrual rows.
    When Admin sets the business date to "04 February 2022"
    When Admin runs COB job
    Then Admin checks that delinquency range is: "RANGE_1" and has delinquentDate "2022-02-03"
    Then Loan Repayment schedule has the following data in Total row:
      | Principal due | Interest | Fees | Penalties | Due    | Paid  | In advance | Late | Outstanding |
      | 1000.0        | 0.0      | 0.0  | 0.0       | 1000.0 | 500.0 | 500.0      | 0.0  | 500.0       |
    Then Loan Transactions tab has the following data:
      | Transaction date | Transaction Type | Amount | Principal | Interest | Fees | Penalties | Loan Balance |
      | 01 January 2022  | Disbursement     | 1000.0 | 0.0       | 0.0      | 0.0  | 0.0       | 1000.0       |
      | 15 January 2022  | Repayment        | 500.0  | 500.0     | 0.0      | 0.0  | 0.0       | 500.0        |

    # --- LATE full payoff (04 Feb, after due) -----------------------------
    # This 500 lands in the "Late" column (paid after 31 Jan due date).
    # Final schedule shows both in-advance 500 and late 500 amounts.
    And Customer makes "AUTOPAY" repayment on "04 February 2022" with 500 EUR transaction amount
    Then Loan Repayment schedule has 1 periods, with the following data for periods:
      | Nr | Days | Date            | Paid date        | Balance of loan | Principal due | Interest | Fees | Penalties | Due    | Paid   | In advance | Late  | Outstanding |
      |    |      | 01 January 2022 |                  | 1000.0          |               |          | 0.0  |           | 0.0    | 0.0    |            |       |             |
      | 1  | 30   | 31 January 2022 | 04 February 2022 | 0.0             | 1000.0        | 0.0      | 0.0  | 0.0       | 1000.0 | 1000.0 | 500.0      | 500.0 | 0.0         |
    Then Loan Repayment schedule has the following data in Total row:
      | Principal due | Interest | Fees | Penalties | Due    | Paid   | In advance | Late  | Outstanding |
      | 1000.0        | 0.0      | 0.0  | 0.0       | 1000.0 | 1000.0 | 500.0      | 500.0 | 0.0         |
    Then Loan Transactions tab has the following data:
      | Transaction date | Transaction Type | Amount | Principal | Interest | Fees | Penalties | Loan Balance |
      | 01 January 2022  | Disbursement     | 1000.0 | 0.0       | 0.0      | 0.0  | 0.0       | 1000.0       |
      | 15 January 2022  | Repayment        | 500.0  | 500.0     | 0.0      | 0.0  | 0.0       | 500.0        |
      | 04 February 2022 | Repayment        | 500.0  | 500.0     | 0.0      | 0.0  | 0.0       | 0.0          |
    Then Loan has 0 outstanding amount
    Then Loan status will be "CLOSED_OBLIGATIONS_MET"

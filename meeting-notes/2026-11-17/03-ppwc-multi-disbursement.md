# PPWC – Multiple disbursements on the same date (refinance with net funding)

| | |
|---|---|
| **Product** | Working capital loan (PPWC) |
| **Requested by** | Logu (PayPal), 2026-11-17 |
| **Priority** | 3 of 3 (after charge refund and chargeback). "Good to do, not right away" |
| **Status** | Business need understood. Modelling decision pending an effort estimate |

## Goal

Let a working capital loan be disbursed in **more than one disbursement transaction on the same transaction date**, each carrying its purpose, while the loan behaves exactly as if the total had been disbursed in one transaction.

Example: approved principal 300K, discount fee 20K, so the customer repays 320K.

| Disbursement | Amount | Purpose |
|---|---|---|
| 1 | 290K | Loan funding, paid to the customer |
| 2 | 10K | Payoff of the customer's old loan |
| Total | 300K | Contractual principal. EIR, projection and discount fee amortization are based on it |

## Why are we doing it?

- **Refinance or renewal with net funding.** A customer who has repaid about 90% of an old loan qualifies for a new one. Today PayPal disburses the full 300K and asks the customer to schedule a 10K payment on the old loan. Customers often don't, so PayPal has to keep chasing them. With net funding the customer receives 290K and PayPal moves the 10K to the old loan internally.
- **Reconciliation across systems.** The old loan may be in Fineract, FDR or a legacy system in a separate legal entity. Splitting the disbursement makes the 290K funding and the 10K payoff visible as separate lines in every system.
- **Consistency.** The interest-bearing loans (MTL) already post the refinance as two disbursements. PayPal wants every product on the credit platform to work the same way.

Fineract does **not** need to know about the old loan or the link between the two loans. PayPal's credit platform posts the payoff on the old loan in whichever system holds it.

## Requirements

### Functional

| ID | Requirement |
|---|---|
| F-1 | A working capital loan can take a **second (and further) disbursement** while it is active, when the product allows it. |
| F-2 | Every disbursement after the first must have the **same transaction date** as the first. Different dates are rejected (see Q-2). The submitted (posting) date may differ. |
| F-3 | The total of all disbursements must not exceed the **approved principal**. |
| F-4 | The **contractual principal = the total disbursed** (300K). The projection, EIR, payment amounts and discount fee amortization must be **identical** to a single 300K disbursement. |
| F-5 | The discount fee stays **loan-level** (one fee for the loan) and is validated against the approved principal or the total disbursed, not against one disbursement. |
| F-6 | Each disbursement carries its **purpose**, for example "Loan funding" or "Payoff of other loan". |
| F-7 | The purpose can drive **GL routing**: the payoff leg can credit a different fund source or clearing account from customer funding. |
| F-8 | Read APIs and events show **each disbursement** (date, amount, purpose) as well as the total disbursed. |
| F-9 | **Undo disbursal** stays all-or-nothing: it reverses every disbursement and returns the loan to Approved. Undoing a single disbursement is not supported. |
| F-10 | **Cancellation:** the customer gets money back through payout refunds, one per disbursement (290K and 10K). No change is needed for this; payout refund already works for any amount. |
| F-11 | Loans with one disbursement, and products without the setting, behave exactly as today. |

### Non-functional

| ID | Requirement |
|---|---|
| NF-1 | **Backward compatible:** opt-in per product, no data migration for existing loans, and only additive changes to Avro and the REST API. |
| NF-2 | **Deterministic reprocessing:** rebuilding the schedule (rate change, discount adjustment, replay) gives the same result whatever order the disbursements were posted in. |
| NF-3 | **Accounting integrity:** every disbursement posts balanced journal entries, and undo reverses all of them. |
| NF-4 | **Idempotency:** the external id is unique per disbursement transaction, the same rule as today. |
| NF-5 | **Command style:** the working capital module uses the legacy command pipeline (`@CommandType` / `NewCommandSourceHandler`) throughout, so the change stays in that style (AGENTS.md: keep to the style the feature already uses). |
| NF-6 | **Tests:** unit tests for the validator and balance, Feign integration tests, and Cucumber E2E scenarios for split, cancel, undo, rate change and COB between the two postings. |

## High Level Design (HLD)

### Current state (develop)

- A working capital loan can be disbursed **only once**. The lifecycle allows `LOAN_DISBURSED` only from Approved (`WorkingCapitalLoanLifecycleStateMachine:110`), and undo disbursal fails when more than one active disbursement exists (`WorkingCapitalLoanWritePlatformServiceImpl:1749`).
- Disbursement **already accepts a classification** (code `working_capital_loan_disbursement_classification`, which is seeded with no values) and **payment details with a payment type**. Accounting already picks the fund source GL by payment type.
- The projection model takes **one net disbursement amount and one date**. Every rebuild reads `getFirstActualDisbursementAmount()`.
- A payout refund is a repayment-like credit. It is not linked to a disbursement.

### Options

| | A. Same-date multi-disbursement (recommended) | B. New transaction type for the payoff |
|---|---|---|
| Model | N `DISBURSEMENT` transactions on the first disbursement date. The purpose comes from the classification, and GL routing from the payment type | A new transaction type that increases principal like a disbursement |
| Projection | Unchanged: the model gets the **sum** | Unchanged, but every rebuild has to add the new type |
| Accounting | No new posting rules. The payment type selects the fund source | New posting rules for the new type |
| Events and Avro | Reuses the disbursement events. Avro changes are additive | New event classes, an external event registration and Avro enum values |
| Allocation, undo, reprocessing | Change the disbursement paths only | Every path needs a new branch |
| Cost | Medium | High |
| Adam's concern ("the 10K is not really a disbursement") | Handled by the classification, for example `PAYOFF_OTHER_LOAN` | Handled by the type itself |

**Recommendation: option A.** It meets Logu's request (multi-disbursement) and Adam's concern (the purpose can be told apart) with the least change. Option B is out of scope unless the effort review shows otherwise.

### Flow (option A)

```
Approved (approved principal 300K, discount 20K)
  │ POST ?command=disburse  290K, date D, classification LOAN_FUNDING, discount 20K
  ▼
Active  ── projection built on 290K (temporary)
  │ POST ?command=disburse  10K, date D, classification PAYOFF_OTHER_LOAN, paymentType PAYOFF
  ▼
Active  ── principal = 300K, balance += 10K, projection rebuilt on 300K at date D
```

Alternatively, both items can be accepted in **one** command (Q-3), which avoids the temporary 290K projection.

## Low Level Design

All paths are relative to `fineract-working-capital-loan/src/main/java/org/apache/fineract/portfolio/`.

### Product setting

- Add to the working capital loan product (`workingcapitalloanproduct/domain/WorkingCapitalLoanProduct*`, `WorkingCapitalLoanProductConstants`, the product validator and write service) a flag `allowMultipleDisbursementsOnSameDate` (default `false`). Optionally add `maxDisbursementCount`.
- Liquibase: a new part after `0094_wc_loan_foreign_key_indexes.xml`, registered in `module-changelog-master.xml`.

### Lifecycle and gate (`WorkingCapitalLoanWritePlatformServiceImpl.disburseLoan`, 327-436)

- First disbursement: unchanged (Approved to Active).
- Further disbursement: if the loan is **Active** and the product flag is on, **skip the state transition**. Otherwise keep today's error.
- Allow a further disbursement only while the loan has no repayment-like, credit or charge-off transactions. Reuse the allowed-type set from `ensureUndoDisbursalAllowed` (1823-1840): disbursement, discount fee (and adjustment), discount fee amortization (and adjustment), accrual.

### Validation (`serialization/WorkingCapitalLoanDataValidator.validateDisbursement`, 556-675)

- Date: must equal the first actual disbursement date (F-2).
- Amount: (already disbursed + this amount) ≤ approved principal (F-3). This replaces the per-transaction check at 610-614.
- Discount: may be sent only once per loan. Validate it against the approved principal or the total disbursed, not this transaction's amount (644-647, and line 241 on the application side).

### Domain

- `WorkingCapitalLoan`: add `getTotalActualDisbursementAmount()` (the sum of disbursed detail rows). Keep `getFirstActualDisbursementDate()`.
- `WorkingCapitalLoanDisbursementDetails`: **one row per disbursement**. The first fills the existing expected row; each further one adds a row with expected = actual. Optionally store the disbursement transaction id.
- `WorkingCapitalLoanBalance.applyDisbursement` (158-165): make it **additive** for principal and `totalDisbursement`. Set `totalDiscountFee` only when the discount is applied.
- `disburseLoan` line 368: `setPrincipal(total disbursed)` instead of `transactionAmount`.

### Projection (`service/WorkingCapitalLoanAmortizationScheduleWriteServiceImpl`)

- `generateAndSaveAmortizationScheduleOnDisbursement` (117-123): on a further disbursement, **regenerate** the model from the total at the first date. Then re-run discount fee amortization for any days already processed by COB (the posting may come a day later).
- `reconstructScheduleModel` (322-339): use `getTotalActualDisbursementAmount()` instead of `getFirstActualDisbursementAmount()`. This covers rate change, discount adjustment and replay.
- No change to `calc/*` (model, `AmortizationWalk`, `TvmFunctions`) as long as different dates are not allowed.

### Discount fee transaction

- `resolveRelatedDisbursementTransaction` (659-663) finds "the" disbursement by date. With two on the same date, link the discount fee to the **first** disbursement (lowest id).
- `repository/WorkingCapitalLoanTransactionRepository.findActiveByTypeAndTransactionDate` (53-59) returns an `Optional`. Change it to return a list, or add an ordered "first" variant.

### Undo disbursal (`undoDisbursal` 439-494, `reverseDisbursementTransactions` 1740-1782)

- Remove the single-active-disbursement guard (1749) and reverse **all** disbursements.
- Raise `WorkingCapitalLoanUndoDisbursalTransactionBusinessEvent` once per reversed disbursement.
- Clear the extra detail rows that were added after the first.

### Purpose and accounting

- Purpose: values of the `working_capital_loan_disbursement_classification` code, for example `LOAN_FUNDING` and `PAYOFF_OTHER_LOAN`. These are tenant data; Fineract seeds none.
- GL routing: a payment type such as "Refinance payoff" with a payment-channel fund source mapping on the product. `AccrualWithDeferredRevenueAmortizationAccountingProcessorForWorkingCapitalLoan` already posts Dr Loan portfolio / Cr Fund source for each disbursement. **No accounting code change.**

### Read side and events

- Mappers: `WorkingCapitalLoanMapper` (171-191), `WorkingCapitalLoanSummaryMapper` (73-76), `WorkingCapitalLoanDisbursementDetailMapper`. Expose every detail row with its actual amount, plus the total.
- Avro: add `actualAmount` to `WorkingCapitalLoanDisbursementDetailDataV1` (nullable). Stop copying the loan-level `netDisbursalAmount` onto every detail row (`WorkingCapitalLoanBusinessEventSerializer:91-92`).
- Events: raise the transaction-level disbursal event for each disbursement. Raise the loan-level `WorkingCapitalLoanDisbursalBusinessEvent` for each disbursement or only the first (Q-5).
- Swagger: `WorkingCapitalLoanApiResourceSwagger` (request 820-849, detail 511).

### Tests

- Unit tests: the validator (date, cumulative amount, discount), additive balance, `reconstructScheduleModel` with two rows.
- Feign integration tests: 290K + 10K gives a schedule equal to a single 300K disbursement; a different date is rejected; total above approved is rejected; undo reverses both; two payout refunds.
- E2E: COB runs between the two postings; rate change after the split; GL routing by payment type.

## Decision tracker

| Date | Participants | Decision |
|---|---|---|
| 2026-11-17 | Adam, Bharath, Logu, sarramalingam, Shannon | The old loan stays outside the scope of Fineract. PayPal's credit platform posts the payoff in whichever system holds the old loan |
| 2026-11-17 | Adam, Bharath, Logu, sarramalingam, Shannon | Top-up loan is **not** suitable: the old loan is often in FDR or a legacy system |
| 2026-11-17 | Adam, Bharath, Logu, sarramalingam, Shannon | Both disbursements have the same transaction date. The terms (EIR, maturity) do not change, and the total is the principal for EIR and amortization |
| 2026-11-17 | Adam, Bharath, Logu, sarramalingam, Shannon | Undoing just the 10K disbursement is not needed. Cancellation is done with refunds (290K and 10K) |
| 2026-11-17 | Adam, Bharath, Logu, sarramalingam, Shannon | Priority 3, after charge refund and chargeback |
| *pending* | Logu (after the effort estimate) | Plain multi-disbursement, or a separate transaction type for the payoff? The proposal recommends option A (multi-disbursement with classification) |

## Questions

| # | Question | Owner |
|---|---|---|
| Q-1 | Is option A (multi-disbursement, purpose from the classification, GL routing from the payment type) acceptable? Or is a separate transaction type needed? | Logu |
| Q-2 | Is a same-date-only rule acceptable for the first release? Different dates would need changes to the EIR engine (several outflows in the IRR). | Logu / Bharath |
| Q-3 | Sequential calls (290K, then 10K) or one command with both items? Logu said they are "posted one after another", but one request is also possible for him. | Logu |
| Q-4 | Can the second posting come after COB has run (a later posting date)? If so, the discount fee amortization already posted must be recalculated. | Logu |
| Q-5 | Should the loan-level disbursal event fire for each disbursement or only the first? | Logu (event consumers) |
| Q-6 | Should payout refunds be linked to a specific disbursement (290K refund ↔ 290K disbursement), or are unlinked refunds enough? | Logu |
| Q-7 | GL: should the payoff leg go to a dedicated clearing account (through a payment type mapping)? Which account? | Logu / accounting |
| Q-8 | Should there be a maximum number of disbursements per loan (for example 2)? | Logu |
| Q-9 | Logu to send Bharath the worked example from the call. | Logu |

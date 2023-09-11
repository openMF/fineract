# PPWC – Chargeback on working capital loans

| | |
|---|---|
| **Product** | Working capital loan (PPWC): no interest, discount fee recognized through amortization |
| **Requested by** | Logu (PayPal), 2026-11-17 |
| **Priority** | 2 of 3. Logu: look at this **before** multi-disbursement |
| **Status** | Requirement agreed in principle. Discount fee treatment waits for Alan's decision |

## Goal

Add a **chargeback** transaction to working capital loans. When a repayment is charged back by the payment network, the loan records it **as of the chargeback date (today)**, linked to the original repayment. The original repayment stays in place. The charged-back amount becomes outstanding again and goes back into the amortization.

## Why are we doing it?

- Today the only way to handle a returned repayment on a working capital loan is to **undo or adjust** it. That rewrites history as of the original repayment date: balances, delinquency, discount fee amortization and accounting are restated in the past.
- PayPal recently concluded that repayment reversal is **not ideal** for PPWC. The repayment really happened, and the money was really taken back later. The books should show both events on their own dates.
- Progressive loans already have chargeback. PPWC should behave the same way for operations, reconciliation and event consumers.
- PayPal expects very few cases, but needs the capability.

## Requirements

### Functional

| ID | Requirement |
|---|---|
| F-1 | New **CHARGEBACK** transaction on working capital loans, posted against an existing **non-reversed repayment-type transaction** (at least repayment; other types see Q-1). |
| F-2 | Dated on the **business date**. No transaction date is accepted, the same as for progressive loans. |
| F-3 | The amount must be positive. The **total of all chargebacks of one original must not exceed the original amount** (several partial chargebacks are allowed). |
| F-4 | The chargeback first **uses up any overpayment**. The rest **increases principal outstanding** (no interest is involved). |
| F-5 | It is linked to the original through a `CHARGEBACK` transaction relation, and the read API shows the link. |
| F-6 | Effect on the projection and amortization: the charged-back principal is owed again and is reflected in the projection. The **discount fee treatment** follows the decision on option A or B (see the decision tracker). |
| F-7 | A **closed (obligations met) or overpaid** loan is **reopened** when the chargeback leaves something outstanding. |
| F-8 | While a repayment has a chargeback that is not reversed, it **cannot be undone or adjusted**. The chargeback itself **can be undone**, which restores the state before it. |
| F-9 | Allowed loan statuses: Active, Closed (obligations met) and Overpaid. **Written-off loans are rejected.** Charged-off loans are allowed, with charge-off accounting routing (see Q-4). |
| F-10 | Delinquency and breach are recalculated, because the outstanding amount went up. |
| F-11 | Accounting (deferred revenue amortization rule): **Cr Fund source** for the full amount. **Dr Overpayment liability** for the part that used up overpayment. **Dr Loan portfolio** for the principal part, or Dr charge-off expense / fraud expense on a charged-off loan. |
| F-12 | Business events: a new chargeback transaction event, plus the existing balance-changed event and (if status changes) status-changed event. The chargeback is included in the summary transaction totals. |
| F-13 | The external id can be given and must be unique. |

### Non-functional

| ID | Requirement |
|---|---|
| NF-1 | **Reprocessing safety:** reverse-replay must replay chargebacks. Today `reprocessFully` resets `principalAdjustment` to zero (`WorkingCapitalLoanTransactionReprocessingServiceImpl:253`). A chargeback that is not replayed would be lost silently. |
| NF-2 | **Command style:** legacy `@CommandType` / `NewCommandSourceHandler`, as everywhere else in the working capital module (AGENTS.md: keep to the feature's style). |
| NF-3 | **Permissions:** `CHARGEBACK_WORKINGCAPITALLOANTRANSACTION` (plus `_CHECKER`) seeded through Liquibase. |
| NF-4 | **Batch API and COB lock:** work without changes (the existing transaction strategies and the `/working-capital-loans/{id}` filter already cover a new command). |
| NF-5 | **Avro:** backward compatible. `LoanTransactionEnumDataV1` already has `chargeback`. Any new summary total is a nullable field. |
| NF-6 | **External event configuration:** a new row for the chargeback event, plus updates to the event-type lists used in tests. |
| NF-7 | **Tests:** unit tests (validator, allocation, balance), Feign integration tests, and E2E scenarios for partial and full chargeback, overpaid loans, closed loans, charged-off loans, undo of the chargeback, the blocked undo of a charged-back repayment, the COB discount fee effect and backdated repayments that trigger replay. |

## High Level Design (HLD)

### Current state (develop)

- Working capital loans have **no chargeback**. Undo, adjust and adjust-by-delta are the only ways to take back a repayment.
- Working capital transactions reuse the core `LoanTransactionType` enum, so `CHARGEBACK(25)` and the relation type `CHARGEBACK(1)` **already exist**. They are just not used.
- The nearest existing pattern is the **credit balance refund (CBR) over-refund**: it uses up overpayment first, and the excess goes to `principalAdjustment`. In the projection it is recorded through `ProjectedAmortizationScheduleModel.applyPrincipalAdjustment`, which on purpose stays **outside** the EIR, NPV and fee. Accounting: Dr Overpayment / Dr Loan portfolio, Cr Fund source. That is exactly the chargeback shape.
- Discount fee income follows **principal actually paid** and is posted daily by COB (`DiscountFeeAmortizationBusinessStep`). When a repayment is undone, the income already recognized stays, and the **next COB posts a discount fee amortization adjustment** (E2E: `WorkingCapitalLoanUndoTransaction.feature:441`).

### API

```
POST /v1/working-capital-loans/{loanId}/transactions/{transactionId}?command=chargeback
POST /v1/working-capital-loans/{loanId}/transactions/external-id/{externalId}?command=chargeback
POST /v1/working-capital-loans/external-id/{loanExternalId}/transactions/...?command=chargeback
{ "transactionAmount": 50.00, "externalId": "...", "note": "...", "paymentTypeId": 1, "locale": "en" }
```

### Flow

```
Repayment R (day 10, 100)          Chargeback C (today, 50) ──CHARGEBACK relation── R
        │                                     │
        │                         1. validate: R repayment-type, not reversed, Σ chargebacks(R)+50 ≤ 100,
        │                            loan status allowed, not written off
        │                         2. allocate: use up overpayment first, rest → principalAdjustment
        │                         3. projection: principal adjustment at today (+ discount fee per option A/B)
        │                         4. delinquency / breach recalculation, status: reopen if closed or overpaid
        │                         5. journal entries: Dr Overpayment / Dr Loan portfolio, Cr Fund source
        ▼                         6. events: chargeback txn, balance changed, status changed
```

### Discount fee: the two options (to be decided by Alan)

| | **A – Reverse the earned income, then recalculate** | **B – Principal only** |
|---|---|---|
| Behaviour | The discount fee income recognized on the charged-back amount is reversed. The next repayments amortize the discount fee again on the new balance | The charged-back amount simply goes back to principal. Income already recognized stays. Repaying the chargeback amount earns no discount fee income |
| Economic meaning | Income follows money actually kept. This matches what **undo** already does | Treats the chargeback as a separate principal re-injection, like a CBR over-refund |
| How in the projection | The chargeback reduces "principal paid" as of the chargeback date (a partial, dated negative payment). The next COB then posts the `DISCOUNT_FEE_AMORTIZATION_ADJUSTMENT` automatically | `applyPrincipalAdjustment` (exists today, outside the EIR/NPV/fee). No amortization change |
| New engine work | **Yes**: a new model operation for a partial reversal of paid principal as of a date (`undoPayment` today only matches an exact payment). It goes into `AmortizationWalk` / `PlanCursor` and into replay | **No**: reuses the CBR path |
| Effort | Medium–high | Low–medium |

**Proposal:** **option A** as the target, because it is economically correct, consistent with undo and gives the community the same result whichever way a returned payment is handled. If the effort is too high for the PayPal timeline, use **B** first and A later. Adam emails Alan for the decision (agreed in the call).

## Low Level Design

All paths are relative to `fineract-working-capital-loan/src/main/java/org/apache/fineract/portfolio/workingcapitalloan/` unless a full path is given.

### API and command

- `api/WorkingCapitalLoanTransactionsApiResource.java`: add `chargeback` to the per-transaction command dispatch (about lines 400-431) and to the command description.
- `WorkingCapitalLoanConstants`: `CHARGEBACK_COMMAND = "chargeback"`.
- `fineract-core/.../commands/service/CommandWrapperBuilder.java`: `chargebackWorkingCapitalLoanTransaction(loanId, transactionId)`, plus external id variants (`ACTION_CHARGEBACK` already exists).
- New `handler/ChargebackWorkingCapitalLoanTransactionCommandHandler`: `@CommandType(entity = "WORKINGCAPITALLOANTRANSACTION", action = "CHARGEBACK")`, `@Transactional`.

### Validation (`serialization/WorkingCapitalLoanDataValidator`)

- New `validateChargeback`. Accepted parameters: `transactionAmount`, `externalId`, `note`, `paymentTypeId`, `locale`.
- Checks:
  - the original is repayment-type and not reversed
  - Σ non-reversed chargebacks linked to it + amount ≤ original amount
  - the amount fits the currency scale
  - status is in {Active, Closed obligations met, Overpaid}, and the loan is not written off
- `validateTransactionReversible` (863-875): reject undo or adjust of an original that has a chargeback that is not reversed.

### Domain

- `domain/WorkingCapitalLoanTransaction`: factory `chargeback(loan, amount, businessDate, paymentDetail, externalId)`.
- `domain/WorkingCapitalLoanTransactionAllocation`: reuse the CBR shape (`forCreditBalanceRefund` 132-141): `overpaymentPortion` holds the overpayment used, `principalPortion` the principal re-injected.
- Relation: the WC convention is **from = new transaction → to = original** (`saveNewTransactionRelation`). Core chargeback goes original → chargeback. Pick one for WC and keep it consistent in the relation repository queries.

### Write service (`service/WorkingCapitalLoanWritePlatformServiceImpl`)

- New `chargebackTransaction(loanId, transactionId, command)`:
  1. Resolve the original by id or external id (`findTransactionToReverse` pattern).
  2. Validate.
  3. Create the transaction and relation.
  4. Allocate: overpayment first, then `balance.principalAdjustment += rest`.
  5. Projection: `applyPrincipalAdjustment(today, rest)`. For option A, also reduce principal paid in the model.
  6. Delinquency and breach.
  7. `stateMachine.determineAndTransition`. Reopening needs `maturedOnDate != null`; follow the charge-on-closed-loan path in `WorkingCapitalLoanChargeWritePlatformServiceImpl:157-166, 320-341`, which also regenerates periods and calls `rederiveNearBreachIfReopened`.
  8. `recalculateOverpaidOnDate` and `recalculateSettlementDates`.
  9. Journal entries and events.
- `undoTransaction` switch (815-825): add `CHARGEBACK` with its own reversal (rewind the principal adjustment and give back the overpayment). `updateBalanceAfterUndo` (1681-1708) throws for unknown types, so either add the case or always route through full replay.
- `notifyPostBusinessEvent` (1252-1263): add `CHARGEBACK`.

### Reprocessing (`service/WorkingCapitalLoanTransactionReprocessingServiceImpl`)

- `reprocessFully` (226-338): add `CHARGEBACK` to the replayable set with a `replayChargeback` that works like `replayCreditBalanceRefund` (485-500). Overpayment is taken from the running overpayment and the excess goes to `principalAdjustment`. **Required** because of the reset at line 253.
- `reprocessChargeFreeSuffix` (92-202): fall back to a full replay when a chargeback is in the suffix, as is already done for CBR (114-121).
- `WorkingCapitalLoanAmortizationScheduleWriteServiceImpl.collectPrincipalHistory` (550-573): map `CHARGEBACK` to a `PrincipalAdjustment` (and, for option A, also to the paid-principal reversal).

### Projection and discount fee

- Option B: no change to `calc/*`. Check that a repayment of the re-injected principal does not push the actual amortization past the full discount; the CBR over-refund path already has this property, so reuse its tests.
- Option A: new operation on `ProjectedAmortizationScheduleModel` (and `AmortizationWalk` / `PlanCursor`) that lowers the cumulative principal paid as of a date. `WorkingCapitalLoanDiscountFeeAmortizationServiceImpl.processDiscountFeeAmortization` (66-128) then posts the negative delta as an amortization adjustment on the next COB. Bump `MODEL_VERSION` if the stored model shape changes.

### Delinquency and breach

- `WorkingCapitalLoanDelinquencyRangeScheduleServiceImpl` (about line 166) and `WorkingCapitalLoanBreachScheduleServiceImpl` (177, 221) add up gross repayment-type amounts. Net chargebacks against the period they fall in, or apply them like `applyRepaymentUndo` (see Q-6).

### Accounting

- `fineract-provider/.../accounting/journalentry/service/AccrualWithDeferredRevenueAmortizationAccountingProcessorForWorkingCapitalLoan.plannedPostings` (133-158): add a `CHARGEBACK` case.
  - Cr FUND_SOURCE (by payment type).
  - Dr OVERPAYMENT (overpayment portion).
  - Dr LOAN_PORTFOLIO (principal portion). After charge-off, use CHARGE_OFF_EXPENSE, or CHARGE_OFF_FRAUD_EXPENSE when the loan is flagged as fraud (`isAfterActiveChargeOffForAccountingRouting`).
- An unknown type throws `NotImplementedException` today, so this case is required.
- Reversal and restatement use the same matrix.

### Events and Avro

- New `infrastructure/event/business/domain/workingcapitalloan/transaction/WorkingCapitalLoanChargebackTransactionBusinessEvent`, registered in `WorkingCapitalLoanTransactionTypeExternalEvents`.
- `WorkingCapitalLoanBusinessEventSerializer.populateSummaryTransactionTypeTotals` (124-143): add chargeback. Optionally add a nullable `totalChargeback` to `WorkingCapitalLoanSummaryDataV1`.

### Liquibase (`.../workingcapitalloan/parts/0095_wc_loan_chargeback.xml`)

- Permission rows (`CHARGEBACK_WORKINGCAPITALLOANTRANSACTION`, `_CHECKER`).
- External event configuration row.
- Include the part in the module changelog master.

### Tests

- Unit tests: validator, allocation (overpayment / principal split), replay with a chargeback, accounting postings.
- Feign integration tests: partial and full chargeback, cumulative limit, overpaid → active, closed → active, charged-off routing, written-off rejected, undo of the chargeback, blocked undo of the original.
- E2E (`fineract-e2e-tests-runner/.../features/WorkingCapital*`): COB discount fee effect under the chosen option, events (`EventCheckHelper`), journal entries.
- Event-type lists: `ExternalEventConfigurationValidationServiceTest`, `ExternalEventConfigurationTestData`.

## Decision tracker

| Date | Participants | Decision |
|---|---|---|
| 2026-11-17 | Adam, Bharath, Logu, sarramalingam, Shannon | PPWC needs a **chargeback** transaction; repayment reversal is not the preferred way to handle a charged-back payment |
| 2026-11-17 | Adam, Bharath, Logu, sarramalingam, Shannon | The chargeback is posted **as of today**, not backdated. No interest is involved; the amount goes back to principal outstanding |
| 2026-11-17 | Adam, Bharath, Logu, sarramalingam, Shannon | The chargeback **affects the amortization calculation** (Logu, in reply to Bharath) |
| 2026-11-17 | Adam, Bharath, Logu, sarramalingam, Shannon | Priority 2: before multi-disbursement, after charge refund |
| 2026-11-17 | Adam, Logu | Discount fee treatment: Adam proposes the option that is best for the implementation and the community. **Alan decides by email.** Logu expects either option to be accepted |
| *pending* | Alan | Discount fee: option A (reverse the earned income and recalculate) or option B (principal only)? The proposal recommends A |

## Questions

| # | Question | Owner |
|---|---|---|
| Q-1 | Which original transactions can be charged back: only **repayment**, or also **payout refund** and **goodwill credit**? (Progressive loans allow repayment, MIR, payout refund, goodwill credit, down payment and interest payment waiver.) | Logu |
| Q-2 | Discount fee: option A or B? | Alan |
| Q-3 | If the original repayment also paid **fees or penalties**: should the chargeback restore those portions (rule-based, like progressive `RULE_BASED` credit allocation), or always go to principal (first release)? | Logu |
| Q-4 | Is a chargeback allowed on a **charged-off** loan? If so, is Dr charge-off expense (or fraud expense) the right posting? | Logu / accounting |
| Q-5 | Is a chargeback allowed on a closed loan long after maturity? If so, which period does it fall in (the projection clamps the date into the term)? | Bharath / Logu |
| Q-6 | Is the charged-back amount **due immediately** and counted in delinquency and breach from today, or only from the next period? | Logu |
| Q-7 | Should the original repayment be blocked from undo or adjust while charged back (as in progressive loans)? Proposal: yes | Adam / Bharath |
| Q-8 | Should a new summary total `totalChargeback` be added to the loan summary and Avro? | Logu (event consumers) |
| Q-9 | Did anything about working capital chargeback already ship? In the call Adam was not sure. **Checked: nothing in develop.** `CHARGEBACK` is not used anywhere in the working capital module | ~~Adam~~ – answered |

# Down payment fee refund: behaviour, accounting and comparison with Interest refund

- **Story:** [PS-3479](https://mifosforge.jira.com/browse/PS-3479), "[BE] PL - Down payment fee refund on Merchant issued refund and Payout refund" (Done). It is blocked by PS-3478 ("Introduce Down payment fee charge type").
- **Apache side:** FINERACT-2939, [apache/fineract#6766](https://github.com/apache/fineract/pull/6766), merged as `9d51971c46`.
- **Code checked:** `origin/develop` at `aa4078d9de` (2026-11-19). Your local `develop` (`726546f303`) is behind it but already contains the merge.

Each statement below says whether it was **checked in code** or comes from the Jira decisions. The examples use simple numbers (0% interest unless stated) so the fee effect is easy to see.

---

## 1. What the feature does

When a **Merchant issued refund (MIR)** or **Payout refund** is posted on a progressive loan, Fineract can now create a linked **Fee refund** transaction (`FEE_REFUND`, id 49). It gives back the share of the **Down payment fee(s)** that matches the share of the loan that was refunded.

```
ratio            = refund amount / total disbursed (disbursements on or before the refund date)
base             = sum of ORIGINAL amounts of active Down payment fees due on or before the refund date
Fee refund       = round(ratio x base)              (loan currency rounding)
cap              = base - Fee refunds already given to EARLIER refunds (in date order)
final Fee refund = min(Fee refund, cap)             no transaction is created if this is 0
```

The code is in `DownPaymentFees.refundAmount(...)` (`fineract-loan/.../loanaccount/domain/DownPaymentFees.java`) and `LoanFeeRefundService`.

Key rules (checked in code):

| Rule | Behaviour |
|---|---|
| Eligible fees | Only charges with charge time **`DOWN_PAYMENT` (17)**. Disbursement, tranche, specified-due-date and installment fees are never refunded. |
| Base | `LoanCharge.amount()`, the **original charged amount**. Waivers, charge adjustments, charge refunds and payments do **not** reduce it. |
| Not-yet-disbursed tranches | A fee whose tranche is not disbursed yet is left out (`isDownPaymentFeeAwaitingDisbursement`). Only disbursements and fees dated on or before the refund date count. |
| Multiple fees / disbursements | One ratio is applied to the sum of all eligible fees. |
| Cap | The total of Fee refunds never goes above the base. The cap is used up in **chronological order** (agreed in Jira, UC27). A backdated refund takes its share first and the later refund gets the rest. |
| Restoring | If the cap becomes free again (for example an earlier MIR is undone), refunds that had no Fee refund get one on the next replay (`ReprocessLoanTransactionsServiceImpl.restoreMissingFeeRefunds`). This was agreed in Jira (UC15: 15 + 2.5 = 17.5). |
| Recalculation | On every reverse-replay the amount is recalculated (`AdvancedPaymentScheduleTransactionProcessor.handleFeeRefund`). If it becomes 0 the Fee refund is reversed. |
| Link | The Fee refund has a `RELATED` relation to its MIR or Payout refund and the same date. |
| Allocation | It is a credit transaction. It uses the product's **`FEE_REFUND`** payment allocation rule when one is configured, otherwise `DEFAULT`. It is **not** tied to the Down payment fee: it pays whatever the rule says (fee, principal, ...) or becomes overpayment. |
| Reversal | Undoing the MIR or Payout refund also reverses the linked Fee refund (`LoanAdjustmentServiceImpl`). A Fee refund cannot be reversed or adjusted directly (`error.msg.loan.transaction.update.not.allowed`). |
| Adjusting the refund | You cannot *adjust* (change the amount of) a MIR or Payout refund when fee refund is enabled for that type or a linked Fee refund exists. Only reversal is allowed. |
| Chargeback of the MIR | The Fee refund stays. This matches Interest refund and was accepted in Jira. |
| Contract-terminated loan | A MIR is accepted and creates a Fee refund as on an active loan (accepted in Jira). There is a separate develop bug where termination replay drops Fee and Interest refunds: [PS-3523](https://mifosforge.jira.com/browse/PS-3523). |
| Events | `LoanTransactionFeeRefundPreBusinessEvent` / `LoanTransactionFeeRefundPostBusinessEvent` |
| Summary | `summary.totalFeeRefund` on the loan |

---

## 2. Configuration

### 2.1 Down payment fee charge (from PS-3478)

- `chargeTimeType = 17` (`DOWN_PAYMENT`), loan charges only
- `chargeCalculationType` must be **FLAT** or **PERCENT_OF_AMOUNT** (`ChargeRulesChecker`)
- Penalties are not allowed
- When auto down payment is on, the automatic Down payment transaction collects the fee together with the down payment principal (`DownPaymentFees.amountToCollect`)

### 2.2 Loan product

```jsonc
POST /loanproducts
{
  "transactionProcessingStrategyCode": "advanced-payment-allocation-strategy", // required
  "loanScheduleType": "PROGRESSIVE",
  "enableDownPayment": true,
  "disbursedAmountPercentageForDownPayment": 25,
  "enableAutoRepaymentForDownPayment": true,

  "supportedFeeRefundTypes": ["MERCHANT_ISSUED_REFUND", "PAYOUT_REFUND"],     // new
  "supportedInterestRefundTypes": ["MERCHANT_ISSUED_REFUND", "PAYOUT_REFUND"], // independent

  "paymentAllocation": [
    { "transactionType": "DEFAULT",    "paymentAllocationOrder": [ ... ], "futureInstallmentAllocationRule": "..." },
    { "transactionType": "FEE_REFUND", "paymentAllocationOrder": [ ... ], "futureInstallmentAllocationRule": "..." } // optional
  ]
}
```

Checked in code:

- Allowed values are `MERCHANT_ISSUED_REFUND` and `PAYOUT_REFUND` (`LoanSupportedFeeRefundTypes`).
- The field is accepted only with `advanced-payment-allocation-strategy`. Otherwise you get `supported.only.for.progressive.loan.schedule.handling` ("Automatic fee refund functionality is only supported for Progressive loans"). Interest refund has the same rule.
- The value is stored in `m_product_loan.supported_fee_refund_types` and copied to `m_loan.supported_fee_refund_types` (Liquibase `1039_add_fee_refund_support.xml`). The loan uses the product value. There is no loan-level override in the loan application API, which is also the case for `supportedInterestRefundTypes`.
- Fee refund and Interest refund are configured **separately**. Either one can be on without the other.
- **Differences from Interest refund:**
  - There is **no per-request override.** A MIR or Payout refund request can switch Interest refund off or on (`interestRefundCalculation`), but it has no such switch for Fee refund.
  - There is **no manual Fee refund API.** Interest refund has `POST /loans/{id}/transactions/{txId}?command=manualInterestRefund`.

---

## 3. Accounting

### 3.1 Journal entries for Fee refund (accrual-based, checked in code)

`AccrualBasedAccountingProcessorForLoan` sends `FEE_REFUND` to `createJournalEntriesForIncomeReducingCredit`. This is the same method used for **Interest refund** and **Interest payment waiver**; only the debit account and the charge-off credit account differ.

| Loan state | Debit | Credit (one line per allocated portion) |
|---|---|---|
| Not charged off | **Income from fees** (product mapping `INCOME_FROM_FEES`) | principal → Loan portfolio · interest → Interest receivable · fee → Fees receivable · penalty → Penalties receivable · overpayment → Overpayment liability |
| Charged off | **Income from fees** | principal, interest, fee and penalty → **Income from charge-off fees** · overpayment → Overpayment liability |

The **whole Fee refund amount is debited to fee income**, whichever portion it pays. If it pays principal, the entry is Dr Fee income / Cr Loan portfolio.

**Cash-based products:** `CashBasedAccountingProcessorForLoan` has no branch for `FEE_REFUND`, `INTEREST_REFUND` or `INTEREST_PAYMENT_WAIVER`, so **no journal entries are posted**. This was agreed in Jira ("follow Interest refund"). Be aware that the loan balance still changes while the GL does not.

**Charge-specific GL mapping:** the Fee refund debit uses the **product-level** "Income from fees" account. If the Down payment fee has its own income account mapping ("Fees to income account" mapping on the product), the accrual and any Charge adjustment use that charge-specific account. The Fee refund still debits the generic one, so the two accounts can disagree.

### 3.2 Example A: single disbursement, fee paid by auto down payment

Product: 25% auto down payment, Down payment fee flat 25, 0% interest, accrual accounting, `supportedFeeRefundTypes = [MERCHANT_ISSUED_REFUND]`.

| Date | Event | Journal entries |
|---|---|---|
| 01 Jan | Disbursement 1,000 | Dr Loan portfolio 1,000 / Cr Fund source 1,000 |
| 01 Jan | Down payment fee accrual 25 | Dr Fees receivable 25 / Cr Income from fees 25 |
| 01 Jan | Auto down payment 275 (250 principal + 25 fee) | Dr Fund source 275 / Cr Loan portfolio 250, Cr Fees receivable 25 |
| 15 Jan | **MIR 500** → ratio 500 / 1,000 = 50% | Dr Fund source 500 / Cr Loan portfolio 500 |
| 15 Jan | **Fee refund 12.50** (50% × 25), allocated to principal | Dr **Income from fees 12.50** / Cr Loan portfolio 12.50 |

Outstanding principal is 750 − 500 − 12.50 = 237.50. Net fee income is 12.50.

**Same loan, full MIR 1,000 on 15 Jan instead:** the ratio is 100%, so the Fee refund is 25. The MIR pays the 750 principal and leaves 250 overpaid. The Fee refund 25 has nothing left to pay and becomes overpayment: Dr Income from fees 25 / Cr Overpayment liability 25. Total overpaid is 275, which is the down payment plus the fee. It goes back to the customer through a Credit balance refund. Net fee income is 0.

### 3.3 Example B: multiple disbursements (from the story)

| | Disbursed | Down payment | Down payment fee |
|---|---|---|---|
| Tranche 1 | 1,000 | 250 | 25.00 |
| Tranche 2 | 500 | 125 | 12.50 |
| **Total** | **1,500** | | **37.50** |

MIR 750 after both tranches: ratio 50%, Fee refund = 50% × 37.50 = **18.75**. The JE is Dr Income from fees 18.75 / Cr (whatever it was allocated to).
If the MIR is dated **between** the two tranche dates, only tranche 1 counts: 750 / 1,000 = 75% × 25 = 18.75. The cap is 25.

### 3.4 Example C: several refunds and the cap

1,000 disbursed, fee 25.

| Refund | Ratio | Uncapped | Cap left | Fee refund |
|---|---|---|---|---|
| MIR 600 (10 Jan) | 60% | 15.00 | 25.00 | **15.00** |
| MIR 600 (12 Jan) | 60% | 15.00 | 10.00 | **10.00** |
| MIR 100 (14 Jan) | 10% | 2.50 | 0.00 | none |

If the 10 Jan MIR is undone, its Fee refund is reversed. The 12 Jan Fee refund becomes 15 and the 14 Jan MIR gets a new Fee refund of 2.50. The total is 17.50.

### 3.5 Example D: Down payment fee still outstanding

If the fee is not paid yet (no auto down payment, or a manual down payment that did not cover the fee) and the `FEE_REFUND`/`DEFAULT` rule puts fees before principal, the Fee refund can pay the fee itself:
Dr Income from fees / Cr Fees receivable. This effectively cancels the accrued income.
In practice the MIR is processed first and usually pays the overdue fee already (noted in the Jira QA comment).

### 3.6 Charged-off loan

Fee refund 12.50 on a charged-off loan: Dr Income from fees 12.50 / Cr **Income from charge-off fees** 12.50. Any overpayment portion goes to Cr Overpayment liability.
This matches the agreed mapping. Interest refund uses Dr Interest income / Cr Income from charge-off interest.

---

## 4. Charge adjustment, Charge refund and Fee refund on the same Down payment fee

### 4.1 The three transactions

| | **Charge adjustment** (`CHARGE_ADJUSTMENT`, 26) | **Charge refund** (`CHARGE_REFUND`, 24) | **Fee refund** (`FEE_REFUND`, 49) |
|---|---|---|---|
| Created by | User, `POST /loans/{id}/charges/{loanChargeId}?command=adjustment` | User, `POST /loans/{id}/transactions?command=chargeRefund` with `loanChargeId` | System, together with MIR / Payout refund |
| Linked to the fee? | Yes, `CHARGE_ADJUSTMENT` relation to the loan charge | Yes, a negative `LoanChargePaidBy` on the charge | **No**, only a `RELATED` link to the MIR |
| Date | Always the business date | Request date | MIR date |
| Amount limit | ≤ charge amount − earlier non-reversed adjustments **of that charge** | ≤ charge **amount paid** − earlier charge refunds of that charge | ratio × **original** fee amount, capped by earlier Fee refunds only |
| Allocation (progressive) | `CHARGE_ADJUSTMENT` rule / DEFAULT (does not have to pay that fee) | `CHARGE_REFUND` rule / DEFAULT | `FEE_REFUND` rule / DEFAULT |
| Loan status | Active, closed (obligations met) or overpaid | Active, closed (obligations met) or overpaid | Follows the MIR |
| Accrual JE | Cr allocated portions / Dr **charge-specific** fee income (falls back to product "Income from fees") | Dr Fund source / Cr allocated portions **plus** Dr Income from fees / Cr Fund source. Net: Dr Fee income / Cr portions | Dr product "Income from fees" / Cr allocated portions |
| Charged-off JE | Dr charge-specific fee income / Cr Income from charge-off fees (principal, interest, fee portions), Income from charge-off penalty, Overpayment | Same as a charged-off repayment, plus the income pair | Dr Income from fees / Cr Income from charge-off fees |

All three **reduce fee income and credit the loan**. Their net accounting is the same; only the GL lines and the chosen income account differ.

### 4.2 Is there validation across them? **No** (checked in code)

- `LoanChargeValidator.validateChargeAdjustmentAmount` only counts earlier **charge adjustments** of the same charge. It does not look at Fee refunds or Charge refunds.
- `loanChargeValidateRefundAmount` (Charge refund) only looks at **amount paid** and earlier **charge refunds**. A Fee refund or Charge adjustment that paid the fee through allocation even **increases** the amount paid, and with it the refundable amount.
- `DownPaymentFees.refundAmount` (Fee refund) uses the **original** fee amount. It does not look at adjustments, charge refunds or waivers. This was accepted in Jira (UC28/UC29, "after comparison with Interest Refund behaviour - PASS").

The only rule that ties them together is the **chronology rule for Charge refund**. No repayment-like transaction (repayment, MIR, Payout refund, Goodwill credit, Charge adjustment, Down payment, Interest payment waiver, Interest refund, Fee refund, ...) may be **created or reversed with a date before an existing non-reversed Charge refund**. The errors are `loan.transaction.cant.be.created.because.later.charge.refund.exists` and `...cant.be.reversed...`. So:

- A MIR backdated before a Charge refund is rejected, so no Fee refund is created for it.
- A MIR (and its Fee refund) dated before a later Charge refund cannot be undone until that Charge refund is reversed. The reversal cascade checks this for every linked Fee refund too.

### 4.3 Worked example: the same 25 fee credited three times

Setup as Example A: 1,000 disbursed, the auto down payment of 275 paid the 25 fee, 750 principal left, 0% interest, accrual accounting.

| Date | Transaction | Validation | Allocation | Net JE |
|---|---|---|---|---|
| 05 Jan | Charge adjustment **10** on the DP fee | 10 ≤ 25 − 0 ✔ | fee already paid → principal | Dr Fee income (charge-specific) 10 / Cr Loan portfolio 10 |
| 06 Jan | Charge refund **25** on the DP fee | 25 ≤ paid 25 − refunded 0 ✔ | principal | net Dr Income from fees 25 / Cr Loan portfolio 25 |
| 10 Jan | MIR **1,000** | not before the Charge refund ✔ | principal 715, overpaid 285 | Dr Fund source 1,000 / Cr Loan portfolio 715, Cr Overpayment 285 |
| 10 Jan | Fee refund **25** (100% × original 25) | none | overpayment | Dr Income from fees 25 / Cr Overpayment 25 |

Result:

- Credits given against the 25 fee: 10 + 25 + 25 = **60**
- Fee income on this loan: +25 accrued − 60 = **−35**
- Overpaid 310, refunded to the customer, who paid only 275. The customer ends up **35 ahead**.

This is how the code behaves today. A variant (paid fee, Charge adjustment 10, full MIR → 35 returned) was raised in Jira and accepted as consistent with Interest refund.

### 4.4 Recommendations

If the business does not want this result, these are the options:

1. **Net base for Fee refund:** use `amount − waived − charge adjustments − charge refunds` (or cap the cumulative Fee refund by that) in `DownPaymentFees.refundAmount`. This is a small, contained change.
2. **Cross-check the manual actions:** make Charge adjustment and Charge refund on a `DOWN_PAYMENT` charge count the Fee refunds given for that charge. The Fee refund is not linked to the charge today, so this needs either a charge-level relation or the pro-rata split per fee to be stored.
3. **Operational rule:** handle merchant refunds only through MIR / Payout refund with Fee refund enabled, and block or require approval for Charge adjustment and Charge refund on Down payment fees.

Whichever is chosen, it should also be decided whether **Interest refund + Interest payment waiver** need the same protection, because they have the same gap (see section 5).

---

## 5. Fee refund compared with Interest refund

### 5.1 Same principles? Mostly yes

| Aspect | Interest refund (`INTEREST_REFUND`, 33) | Fee refund (`FEE_REFUND`, 49) |
|---|---|---|
| Trigger | MIR / Payout refund, product `supportedInterestRefundTypes` | MIR / Payout refund, product `supportedFeeRefundTypes` |
| Loan type | Progressive + advanced payment allocation only | Same |
| What is refunded | Interest that would not have been charged if the refunded amount had never been owed. Calculated as total interest up to the refund date without the refund minus total interest with it (`InterestRefundService.totalInterestByTransactions`) | Pro-rata share of the original Down payment fee(s): refund / disbursed × fees |
| Implicit cap | Total interest up to the refund date | Total Down payment fee amount, used up in date order |
| Link | `RELATED` to the MIR, same date | Same |
| Type of transaction | Credit, allocation rule `INTEREST_REFUND` or DEFAULT | Credit, allocation rule `FEE_REFUND` or DEFAULT |
| Recalculated on replay | Yes, reversed when it becomes 0 | Yes, reversed when it becomes 0 |
| **Restored when the cap frees up** | **No.** Replay never creates new transactions | **Yes**, `restoreMissingFeeRefunds` |
| **Backdated refund vs. used-up cap** | Calculated against all refunds regardless of date (UC27 behaviour) | Calculated in date order (agreed fix; Jira suggests a separate ticket to align Interest refund) |
| Per-request override | `interestRefundCalculation` on the MIR request | None |
| Manual creation | `command=manualInterestRefund` | None |
| Direct reverse / adjust | Not allowed | Not allowed |
| MIR adjust (amount change) | Blocked when enabled or linked | Blocked when enabled or linked |
| Reversed with the MIR | Yes | Yes |
| Chargeback of MIR | Interest refund stays | Fee refund stays |
| **Accrual debit** | **Income from interest** (`INTEREST_ON_LOANS`) | **Income from fees** (`INCOME_FROM_FEES`) |
| Accrual credit | Receivable / portfolio / overpayment, by allocated portion | Same |
| **Charged-off credit** | **Income from charge-off interest** | **Income from charge-off fees** |
| Cash-based | No JE | No JE |
| Events / summary | `LoanTransactionInterestRefund*`, `totalInterestRefund` | `LoanTransactionFeeRefund*`, `totalFeeRefund` |

### 5.2 Accounting side by side

Both use the same posting method, `createJournalEntriesForIncomeReducingCredit`:

```
                       Interest refund                     Fee refund
Not charged off   Dr Income from interest           Dr Income from fees
                  Cr Interest receivable /          Cr Fees receivable /
                     Loan portfolio /                  Loan portfolio /
                     Overpayment (by allocation)       Overpayment (by allocation)

Charged off       Dr Income from interest           Dr Income from fees
                  Cr Income from charge-off         Cr Income from charge-off
                     interest (+ Overpayment)          fees (+ Overpayment)
```

In both cases the credit side follows the **allocation**, not the source. An Interest refund can credit Fees receivable, and a Fee refund can credit Interest receivable, if the allocation rule sends them there. The debit side always reduces the matching **income** account.

### 5.3 High level: do they behave the same?

**Yes, in structure:** both are automatic, income-reducing credit transactions linked to a MIR or Payout refund, calculated by the system, allocated by rules, recalculated on replay, reversed together with their refund, never edited directly, and posted with the same JE template.

**They differ in:**

1. How the amount is calculated: an interest recalculation for Interest refund, a simple pro-rata of an explicit fee amount for Fee refund.
2. Fee refund is restored when the cap is freed and is calculated in date order. Interest refund is not.
3. Interest refund has a per-request switch and a manual API. Fee refund has neither.
4. The income accounts: interest versus fees, and charge-off interest versus charge-off fees.

---

## 6. Interest payment waiver (`INTEREST_PAYMENT_WAIVER`, 31)

### 6.1 What it does

- It is a **manual credit transaction**: `POST /loans/{id}/transactions?command=interestPaymentWaiver` with `transactionDate`, `transactionAmount` and optional payment details, note and external id.
- It is used to forgive an amount as if the customer had paid it, funded by **interest income** instead of cash.
- It is treated as a **repayment-like** transaction. In the progressive processor it goes through `handleRepayment` and is **allocated by the `INTEREST_PAYMENT_WAIVER` allocation rule** (or DEFAULT). Despite its name it is **not limited to interest**: whatever the rule says gets paid (interest, fees, principal), and anything left over becomes overpayment.
- Validations (`validateLoanTransactionInterestPaymentWaiver`):
  - not in the future
  - client and group active
  - loan active, fully paid or overpaid (rules as for repayments)
  - after disbursement
  - no later Charge refund exists
  - not on a holiday or non-working day (unless allowed)
  - for multi-disbursement loans, total principal repaid must not exceed the disbursed amount (checked after recalculation)
- It can be reversed or adjusted like a repayment.
- It shows up in `summary.totalInterestPaymentWaiver`.
- It is **not** the classic "Waive interest" (`WAIVE_INTEREST`, 4) transaction.

### 6.2 Accounting

It uses the same income-reducing credit posting as Interest refund:

| Loan state | Debit | Credit |
|---|---|---|
| Not charged off | **Income from interest** (full amount) | by allocated portion: Interest receivable, Fees receivable, Penalties receivable, Loan portfolio, Overpayment liability |
| Charged off | **Income from interest** | **Income from charge-off interest** (all portions) + Overpayment liability |
| Cash-based | no JE | no JE |

### 6.3 Examples

1. **Waiving accrued interest.** 10 of interest was accrued (Dr Interest receivable 10 / Cr Income from interest 10). An Interest payment waiver of 10 allocated to interest gives Dr **Income from interest 10** / Cr **Interest receivable 10**. Net interest income is 0 and the receivable is cleared.
2. **Waiver bigger than the interest due.** 10 interest due, IPW 30, allocation order interest → principal: Dr Income from interest 30 / Cr Interest receivable 10, Cr Loan portfolio 20. Interest income goes 20 **below** what was earned on this loan, because the whole waiver is booked against interest income.
3. **Charged-off loan.** IPW 10: Dr Income from interest 10 / Cr Income from charge-off interest 10.

**Relation to Fee refund:** an Interest payment waiver is the manual, interest-funded counterpart of the automatic Interest refund. A Charge adjustment or Charge refund plays the same role next to the automatic Fee refund. Neither pair cross-validates, so an Interest refund plus an Interest payment waiver can also give back more than the interest charged.

---

## 7. Open points / suggestions

1. **Over-crediting of the Down payment fee** by Charge adjustment, Charge refund and Fee refund together (section 4). Decide whether this is acceptable or apply one of the options in 4.4.
2. **Cash-based accounting:** Fee refund, Interest refund and Interest payment waiver post no JE, so the loan balance and the GL drift apart. This was agreed in Jira but should be documented for finance users.
3. **Charge-specific income mapping:** Fee refund debits the product "Income from fees", not the Down payment fee's own income GL (section 3.1).
4. **Align Interest refund** with Fee refund: date-order cap and restoring when the cap frees up (proposed in Jira as a separate ticket).
5. **[PS-3523](https://mifosforge.jira.com/browse/PS-3523):** contract-termination replay ignores Fee and Interest refunds.

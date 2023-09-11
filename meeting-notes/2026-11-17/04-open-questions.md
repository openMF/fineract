# Consolidated open questions

The **Who** column shows who is expected to answer or decide. "Team" means Adam and Bharath, checking the code or doing the analysis. The question ids in topics 2 and 3 match the Questions table in each topic document.

## 1. Charge refund ([details](01-charge-refund.md))

Fee refund is implemented for down payment fees ([PS-3479](https://mifosforge.jira.com/browse/PS-3479)). That settled the earlier questions on fee scope, the multi-fee base, configuration, reversal and reprocessing, and the GL account (Dr Fee income, same as interest refund).

| # | Question | Who |
|---|----------|-----|
| 1.1 | Does the Venmo / pay in three fee have to be amortized daily **and** refunded in proportion? Today the two cannot meet: `DOWN_PAYMENT` fees cannot be amortized (FINERACT-3229), and only down payment fees are refunded. | Logu |
| 1.2 | If yes: allow amortizing down payment fees, or extend fee refund to disbursement fees? | Team |
| 1.3 | Amortized fee + charge adjustment after part of the fee is recognized: what happens? (To design in FINERACT-3229.) | Team |
| 1.4 | Amortized fee + fee refund: does it reduce deferred income or recognized income? (Only relevant if 1.1 is yes.) | Team, then accounting |
| 1.5 | Is a manual fee refund needed (interest refund has `MANUAL_INTEREST_REFUND_TRANSACTION`)? | Logu |
| 1.6 | Bring interest refund in line with fee refund (date-ordered cap, refund created again when the cap frees up)? A separate ticket was offered in the PS-3479 comments. | Team |

## 2. Chargeback on PPWC ([details](02-ppwc-chargeback.md))

Checked: working capital loans have no chargeback on develop today.

| # | Question | Who |
|---|----------|-----|
| Q-1 | Which originals can be charged back: repayment only, or also payout refund and goodwill credit? | Logu |
| Q-2 | Discount fee: option A (reverse the earned income and recalculate) or option B (principal only)? The proposal recommends A. | Alan (by email from Adam) |
| Q-3 | Should the chargeback restore the fee and penalty portions of the original, or go to principal only? | Logu |
| Q-4 | Chargeback on a charged-off loan: allowed? Should it post Dr charge-off expense (or fraud expense)? | Logu / accounting |
| Q-5 | Chargeback long after maturity: which period does it fall in? | Bharath / Logu |
| Q-6 | Is the charged-back amount due immediately for delinquency and breach? | Logu |
| Q-7 | Should the original repayment be blocked from undo or adjust while it is charged back? Proposal: yes. | Adam / Bharath |
| Q-8 | Add a `totalChargeback` total to the loan summary and Avro? | Logu |

## 3. Multiple disbursements on PPWC ([details](03-ppwc-multi-disbursement.md))

Checked: a working capital loan can be disbursed only once on develop today.

| # | Question | Who |
|---|----------|-----|
| Q-1 | Is option A acceptable (multi-disbursement, purpose from the classification, GL routing from the payment type)? Or is a separate transaction type needed? | Logu, after the effort estimate |
| Q-2 | Is a same-date-only rule acceptable for the first release? Different dates need changes to the EIR engine. | Logu / Bharath |
| Q-3 | Sequential calls, or one command with both disbursements? | Logu |
| Q-4 | Can the second posting come after COB has run? | Logu |
| Q-5 | Should the loan-level disbursal event fire for each disbursement or only the first? | Logu |
| Q-6 | Should payout refunds be linked to a specific disbursement? | Logu |
| Q-7 | Which GL account for the payoff leg (through a payment type mapping)? | Logu / accounting |
| Q-8 | Maximum number of disbursements per loan? | Logu |
| Q-9 | Send Bharath the worked example. | Logu |

# Meeting notes – 2026-11-17

Source: Zoom recording transcript `GMT20261117-152922_Recording.transcript.vtt` (about 55 minutes).

## Attendees

- Adam Saghy
- Bharath
- Logu
- sarramalingam
- Shannon (joined late)
- Alan was expected but did not join. Several decisions below are to be confirmed with him.

## Context

- The PayPal side has entered a moratorium ("no more pushes") for a few weeks and is focusing on PPWC (PayPal Working Capital).
- Logu raised three topics. Adam and Bharath will work them out and write the design and proposal on a Confluence page.

## Topics (in Logu's priority order)

| # | Topic | Product | Priority | Notes |
|---|-------|---------|----------|-------|
| 1 | [Charge refund proportional to the disbursement refund](01-charge-refund.md) | Instalment loan with upfront fee and down payment (Logu later calls it "the Venmo one / pay in three") | 1 – highest | **Already implemented** for down payment fees ([PS-3479](https://mifosforge.jira.com/browse/PS-3479)). Still open: how it fits with charge amortization (FINERACT-3229) and whether a manual fee refund is needed |
| 2 | [Chargeback for working capital loans](02-ppwc-chargeback.md) | PPWC | 2 | Not on develop yet. Design written (goal, requirements, HLD, LLD). Discount fee option A or B waits for Alan |
| 3 | [Multiple disbursements on the same date](03-ppwc-multi-disbursement.md) | PPWC | 3 – lowest | Not on develop yet (single disbursement only). Design written. Recommends multi-disbursement with the existing disbursement classification. Waiting for Logu after the effort estimate |

Logu: chargeback "may be higher priority than the disbursement". Charge refund stays first.

## Action items

| Owner | Action |
|-------|--------|
| Logu | Send Bharath the refinance / net funding example (290K + 10K) shown during the call |
| Logu | Resend the original charge refund document (it may contain earlier questions that Alan already answered) |
| Adam | Create a Confluence page with the requirements and the proposal for all three topics |
| Adam / Bharath | Check how amortization currently behaves on **charge adjustment** and **charge refund** after fee income has already been partly recognized |
| Adam / Bharath | Check the accounting of **interest refund** and **interest payment waiver** (which GL account is debited) and use it as the baseline for charge refund |
| Adam / Bharath | Check whether chargeback is already supported for working capital loans, and how feasible it is if not |
| Adam | Email Alan the two discount fee options for chargeback and ask for a decision |
| Adam / Bharath | Estimate the effort for multi-disbursement on PPWC compared with a separate "payoff" transaction type |
| Bharath / Adam | Discuss the multi-disbursement edge cases offline (different dates, reversal, EIR impact) |

## Consolidated open questions

See [04-open-questions.md](04-open-questions.md).

## Glossary (as used in the call)

| Term | Meaning |
|------|---------|
| PPWC | PayPal Working Capital loan (Fineract working capital loan product, no interest, discount fee) |
| MTL | Interest-bearing term loans (multi-disbursement already allowed) |
| MIR / "MAR" | Merchant Issued Refund transaction |
| Payout refund | Payout Refund transaction |
| SOR | System of record (Fineract, FDR and other legacy systems sit side by side) |
| FDR | Legacy loan system of record that can hold the "old" loan in a refinance |
| Credit platform | PayPal's orchestration layer that links old and new loans across SORs |
| Discount fee | Upfront fee on a working capital loan, recognized through amortization |
| EIR | Effective interest rate (working capital: calculated on the agreed amount) |
| Double booking | Allowing a refund or waiver even if it makes the loan overpaid |

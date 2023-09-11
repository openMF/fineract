# 1. Charge refund proportional to the disbursement refund

**Priority:** 1 (highest)
**Status:** implemented as [PS-3479](https://mifosforge.jira.com/browse/PS-3479) for down payment fees. The amortization interaction is still open (see "Gaps compared with the meeting")
**Raised by:** Logu (Alan confirmed it earlier; Logu repeated it in this call)
**Product:** instalment loan with an upfront fee and a down payment. Later in the call Logu calls it "the Venmo one ... the pay in three" and links it to the daily amortization of charges.

## Background

- Until now only **charge adjustment** existed. This introduces **charge refund** for the first time.
- The fee in question is tied to the disbursement: a disbursement or origination fee, either calculated by Fineract or sent in the API request. A down payment is created as part of the same flow.
- When the customer gets money back through a disbursement refund, a proportional part of the fee should be refunded too.
- The model is the existing **interest refund**: interest refund calculates what the interest would have been had the lower amount been disbursed in the first place. The fee is the upfront equivalent of interest, so it should work the same way.

## Example

| Step | Amount |
|------|--------|
| Disbursement | $100 |
| Fee linked to the disbursement | $5 |
| Disbursement refund | $50 (50% of the original disbursement) |
| **Charge refund** | **$2.50** (50% of $5) |

Adam's restatement, which Logu confirmed: down payment fee $20, half the disbursement refunded, so a $10 charge refund, no matter what charge adjustments were made earlier.

## Business requirements

| ID | Requirement |
|----|-------------|
| CR-1 | Add a new **charge refund** credit transaction. |
| CR-2 | The charge refund is **linked to the disbursement refund transaction** (merchant issued refund / payout refund), not to the original charge. This is the same as how interest refund is linked to its refund transaction. |
| CR-3 | Amount = original disbursement-linked fee × (refunded amount ÷ original disbursement amount). |
| CR-4 | The amount is calculated and posted **automatically** when the disbursement refund is posted. |
| CR-5 | **No validation** against the remaining fee balance. It does not matter whether the fee was already adjusted or paid in full. The charge refund may make the loan **overpaid** (double booking is allowed), as with interest refund and interest payment waiver today. |
| CR-6 | **Charge adjustment stays as it is**: it is tied to the original charge and capped at the original charge amount (for example at most $5 on a $5 fee). |
| CR-7 | Make **configurable** which refund transaction types trigger a charge refund. PayPal will use it only for **merchant issued refund**, not for payout refund. |
| CR-8 | (From Adam's description of interest refund, to confirm.) Manual entry or override of the refund amount may be allowed, but a manual value can be recalculated when the loan changes later. |

## Implementation status: PS-3479 (done)

[PS-3479](https://mifosforge.jira.com/browse/PS-3479) "PL – Down payment fee refund on Merchant issued refund and Payout refund" was merged in [apache/fineract#6766](https://github.com/apache/fineract/pull/6766) (FINERACT-2939, 2026-11-12). It depends on PS-3478 (the "Down payment fee" charge type).

| Req | Status | How it was built |
|-----|--------|------------------|
| CR-1 | Done | New `FEE_REFUND` transaction type ("Fee refund"), with business events and loan summary figures |
| CR-2 | Done | Linked to the MIR / payout refund through a `RELATED` transaction relation. Reversing the refund reverses the fee refund |
| CR-3 | Done, **down payment fees only** | Refund ÷ total disbursed up to the refund date × the original amount of every down payment fee. Other charge types are explicitly out of scope |
| CR-4 | Done | Created automatically with the refund, recalculated on reprocessing, applied in date order. A refund that got nothing because the cap was used up gets its fee refund once the cap frees up again (Bharath's decision on UC15) |
| CR-5 | Done | Waivers, adjustments and payments do not reduce the base. The credit is allocated like any other credit and can go to overpayment. The only limit is that the total of all fee refunds cannot exceed the total down payment fee amount, the same as interest refund |
| CR-6 | Unchanged | Charge adjustment was not touched |
| CR-7 | Done | `supportedFeeRefundTypes` on the loan product and the loan, mirroring `supportedInterestRefundTypes` |
| CR-8 | **Not done** | Interest refund has a manual command (`MANUAL_INTEREST_REFUND_TRANSACTION`). Fee refund has no manual or override path |

Accounting (accrual only, same code path as interest refund and interest payment waiver):

- Active loan: **Dr Fee income**. Cr fees receivable, loan portfolio or overpayment, depending on what the credit was allocated to.
- Charged-off loan: Dr Fee income, Cr income from charge-off fees.
- Cash-based accounting: no journal entries, as for interest refund (agreed in the Jira comments).

## Gaps compared with the meeting

1. **Charge amortization and fee refund never meet.** FINERACT-3229 (charge amortization, in progress on this branch) allows amortization only for `DISBURSEMENT`, `TRANCHE_DISBURSEMENT` and `SPECIFIED_DUE_DATE` fees and rejects `DOWN_PAYMENT`. Fee refund handles only down payment fees. So an amortized fee is never refunded, and a refunded fee is never amortized. Logu linked "the Venmo / pay in three" charge refund to "the daily amortization ... for a charge". If that fee must be amortized **and** refunded in proportion, one of the two has to change: allow amortizing down payment fees, or extend fee refund to disbursement fees. Confirm with Logu which charge type the product uses.
2. **Amortization questions are still open, for FINERACT-3229.** Only the configuration is implemented so far (the charge definition and the product flag). The deferred-income processing that will answer Logu's questions is not built yet: what a charge adjustment does after part of the fee was recognized, and what a fee refund does to an amortized fee.
3. **No manual fee refund (CR-8).** It was not required in the meeting; Adam mentioned it only while describing interest refund. Decide whether it is needed.
4. **Fee scope wording.** In the meeting Logu said "disbursement fee or origination fee"; PS-3479 covers only the down payment fee. This is consistent with Adam's restatement ("for the down payment"), but it is the same point as gap 1.

Follow-ups raised in the PS-3479 comments:

- Interest refund still behaves the old way: it does **not** create a refund again when the cap frees up, and it does not apply backdated refunds in date order. Mariia offered a separate ticket to bring interest refund in line with fee refund. Check whether that ticket exists.
- [PS-3523](https://mifosforge.jira.com/browse/PS-3523): contract termination replay drops interest refunds and fee refunds (a bug on develop). UC38 is skipped until it is fixed.
- MIR and payout refund are allowed on contract-terminated loans (Bharath confirmed), and fee refund follows the MIR.

## Points raised in the call

- Adam: charge adjustment and interest adjustment are validated (you cannot adjust more than the original amount). Logu: **interest payment waiver** has no such validation, and he offered to demonstrate it.
- Logu: charge refund cases are rare (according to Alan), but the capability is needed.
- Bharath agreed ("thumbs up").

## Amortization and accounting questions (raised near the end of the call)

Logu asked how this interacts with the **daily amortization of charges**:

1. **Charge adjustment after part of the fee income is already recognized.** What happens to the amortization?
2. **Charge refund.** Does it change the amortization?

Adam's first answer (to be checked): amortization is probably not affected. A possible side effect is that **fee income is debited by more than was recognized**, because the refund has to debit some account. Interest payment waiver and interest refund probably already do the same with interest income.

Logu's position on accounting:

- Look at what interest refund and interest payment waiver do today. If doing the same is the simplest option, keep it, because the accounting team has already accepted it.
- If Adam thinks it should be fixed, propose it. Logu will take it to Alan and the accounting team.
- Nobody has reported the interest-side behaviour as a problem so far.

## Open questions

PS-3479 answered several of the questions first noted here: the base is all down payment fees together, the setting is on the product and the loan, the fee refund follows reversal and reprocessing of its refund, and the fee refund debits fee income. Still open:

- Does the Venmo / pay in three fee have to be amortized daily **and** refunded in proportion? If so, which charge type should it be (see gap 1)?
- Once FINERACT-3229 amortization processing is built: what does a charge adjustment do to an amortized fee after part of it is recognized? Does a fee refund on an amortized fee reduce deferred income or recognized income?
- Is a manual fee refund (like manual interest refund) needed?
- Should interest refund be brought in line with fee refund (date-ordered cap, refund created again when the cap frees up)?

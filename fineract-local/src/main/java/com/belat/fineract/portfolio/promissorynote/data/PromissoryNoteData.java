package com.belat.fineract.portfolio.promissorynote.data;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.fineract.portfolio.savings.data.SavingsAccountData;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
public class PromissoryNoteData {

    private SavingsAccountData fundSavingsAccount;
    private SavingsAccountData investorSavingsAccount;
    private BigDecimal investmentAmount;
    private String status;
    private String promissoryNoteNumber;
    private String currencyCode;
}

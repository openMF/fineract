/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.fineract.portfolio.loanaccount.service.fund;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.math.BigDecimal;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Objects;
import lombok.RequiredArgsConstructor;
import org.apache.fineract.infrastructure.core.api.JsonCommand;
import org.apache.fineract.infrastructure.core.data.CommandProcessingResult;
import org.apache.fineract.infrastructure.core.service.DateUtils;
import org.apache.fineract.portfolio.account.domain.AccountAssociationType;
import org.apache.fineract.portfolio.account.domain.AccountAssociations;
import org.apache.fineract.portfolio.account.domain.AccountAssociationsRepository;
import org.apache.fineract.portfolio.client.service.ClientWritePlatformService;
import org.apache.fineract.portfolio.loanaccount.domain.Loan;
import org.apache.fineract.portfolio.savings.domain.SavingsAccount;
import org.apache.fineract.portfolio.savings.domain.SavingsAccountRepository;
import org.apache.fineract.portfolio.savings.exception.SavingsAccountNotFoundException;
import org.apache.fineract.portfolio.savings.service.SavingsApplicationProcessWritePlatformService;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
@Transactional
public class LoanFundServiceImpl {

    private final ApplicationContext applicationContext;
    private final AccountAssociationsRepository accountAssociationsRepository;
    private final ClientWritePlatformService clientWritePlatformService;
    private final SavingsApplicationProcessWritePlatformService savingsApplicationProcessWritePlatformService;
    private final SavingsAccountRepository savingsAccountRepository;

    public void createFundAccount(final Loan loan, final JsonCommand command) {

        // To format date in specific format
        DateTimeFormatter formatter = DateUtils.DEFAULT_DATE_FORMATTER;

        // Build client data
        JsonObject clientJson = createFundClientData(loan.getOfficeId(), loan.getAccountNumber(), formatter);

        JsonCommand clientCommand = JsonCommand.from(String.valueOf(clientJson), JsonParser.parseString(clientJson.toString()),
                command.getFromApiJsonHelper());

        // Create client
        CommandProcessingResult result = clientWritePlatformService.createClient(clientCommand);

        // Build saving data
        JsonObject savingJson = createSavingAccountData(loan.getApprovedPrincipal(), result.getClientId(),
                clientJson.get("fullname").getAsString(), formatter);

        JsonCommand savingCommand = JsonCommand.from(String.valueOf(savingJson), JsonParser.parseString(savingJson.toString()),
                command.getFromApiJsonHelper());

        // Create saving account
        CommandProcessingResult savingResult = savingsApplicationProcessWritePlatformService.submitApplication(savingCommand);

        SavingsAccount account = savingsAccountRepository.findSavingAccountByClientId(result.getClientId()).stream()
                .filter(saving -> Objects.equals(saving.getId(), savingResult.getSavingsId())).findFirst()
                .orElseThrow(() -> new SavingsAccountNotFoundException("Saving account not found"));

        // Link fund saving account
        AccountAssociations accountAssociations = AccountAssociations.associateSavingsAccount(loan, account,
                AccountAssociationType.LINKED_ACCOUNT_ASSOCIATION.getValue(), true);
        this.accountAssociationsRepository.save(accountAssociations);
    }

    private JsonObject createFundClientData(final Long officeId, final String accountNo, final DateTimeFormatter formatter) {
        JsonObject clientJson = new JsonObject();
        clientJson.addProperty("officeId", officeId);
        clientJson.addProperty("legalFormId", applicationContext.getEnvironment().getProperty("fineract.client.type.entity"));
        clientJson.addProperty("fullname",
                Objects.requireNonNull(applicationContext.getEnvironment().getProperty("fineract.fund.client.name"))
                        .concat(String.valueOf(accountNo)));
        clientJson.addProperty("dateFormat", DateUtils.DEFAULT_DATE_FORMAT);
        clientJson.addProperty("locale", Locale.ENGLISH.toString());
        clientJson.addProperty("clientTypeId", applicationContext.getEnvironment().getProperty("fineract.fund.client.type"));
        clientJson.addProperty("activationDate", DateUtils.getBusinessLocalDate().format(formatter));
        clientJson.addProperty("active", true);
        return clientJson;
    }

    private JsonObject createSavingAccountData(final BigDecimal amount, final Long clientId, final String accountNo,
            final DateTimeFormatter formatter) {
        JsonObject accountJson = new JsonObject();
        accountJson.addProperty("minRequiredOpeningBalance", amount);
        accountJson.addProperty("dateFormat", DateUtils.DEFAULT_DATE_FORMAT);
        accountJson.addProperty("locale", Locale.ENGLISH.toString());
        accountJson.addProperty("submittedOnDate", DateUtils.getBusinessLocalDate().format(formatter));
        accountJson.addProperty("productId", applicationContext.getEnvironment().getProperty("fineract.saving.product.id"));
        accountJson.addProperty("clientId", clientId);
        accountJson.addProperty("allowOverdraft", false);
        accountJson.addProperty("accountNo", accountNo);
        return accountJson;
    }

}

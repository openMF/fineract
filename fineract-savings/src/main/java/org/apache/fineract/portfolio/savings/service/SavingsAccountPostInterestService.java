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
package org.apache.fineract.portfolio.savings.service;

import java.math.MathContext;
import java.time.LocalDate;
import org.apache.fineract.portfolio.savings.domain.SavingsAccount;

/**
 * Posts interest onto a {@link SavingsAccount} entity. This orchestration logic was previously a method on the entity
 * itself ({@code SavingsAccount.postInterest}); it has been extracted to keep the domain entity free of process
 * orchestration. The companion {@link SavingsAccountInterestPostingService} performs the equivalent calculation over
 * the {@code SavingsAccountData} DTO used by the batch interest poster.
 */
public interface SavingsAccountPostInterestService {

    /**
     * Posts interest on the given account up to {@code postingDate}. The effective up-to date is resolved
     * polymorphically via {@link SavingsAccount#interestPostingUpToDate(LocalDate)} (identity for regular savings,
     * maturity-capped for recurring deposits), preserving the behaviour previously encoded in the entity's
     * {@code postInterest} overloads.
     */
    void postInterest(SavingsAccount account, MathContext mc, LocalDate postingDate, boolean isInterestTransfer,
            boolean isSavingsInterestPostingAtCurrentPeriodEnd, Integer financialYearBeginningMonth, LocalDate postInterestOnDate,
            boolean backdatedTxnsAllowedTill, boolean postReversals);

}

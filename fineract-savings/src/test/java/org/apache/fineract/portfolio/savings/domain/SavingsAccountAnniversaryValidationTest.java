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
package org.apache.fineract.portfolio.savings.domain;

import static org.apache.fineract.portfolio.savings.SavingsCompoundingInterestPeriodType.ANNUAL;
import static org.apache.fineract.portfolio.savings.SavingsCompoundingInterestPeriodType.BI_ANNUAL;
import static org.apache.fineract.portfolio.savings.SavingsCompoundingInterestPeriodType.DAILY;
import static org.apache.fineract.portfolio.savings.SavingsCompoundingInterestPeriodType.MONTHLY;
import static org.apache.fineract.portfolio.savings.SavingsCompoundingInterestPeriodType.QUATERLY;
import static org.apache.fineract.portfolio.savings.SavingsPostingInterestPeriodType.ANNIVERSARY_ANNUAL;
import static org.apache.fineract.portfolio.savings.SavingsPostingInterestPeriodType.ANNIVERSARY_BIANNUAL;
import static org.apache.fineract.portfolio.savings.SavingsPostingInterestPeriodType.ANNIVERSARY_MONTHLY;
import static org.apache.fineract.portfolio.savings.SavingsPostingInterestPeriodType.ANNIVERSARY_QUARTERLY;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.fineract.infrastructure.core.data.ApiParameterError;
import org.apache.fineract.infrastructure.core.data.DataValidatorBuilder;
import org.apache.fineract.portfolio.savings.SavingsCompoundingInterestPeriodType;
import org.apache.fineract.portfolio.savings.SavingsPostingInterestPeriodType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * SavingsAccount.validateInterestPostingAndCompoundingPeriodTypes only checks that the posting type is a recognized
 * type (present in the map). Unlike FixedDepositProduct, it does NOT restrict which compounding types are allowed per
 * posting type, so any compounding type is valid as long as the posting type itself is recognized.
 */
class SavingsAccountAnniversaryValidationTest {

    private SavingsAccount accountWith(SavingsPostingInterestPeriodType posting, SavingsCompoundingInterestPeriodType compounding) {
        SavingsAccount account = new SavingsAccount() {};
        account.interestPostingPeriodType = posting.getValue();
        account.interestCompoundingPeriodType = compounding.getValue();
        return account;
    }

    private List<ApiParameterError> validate(SavingsPostingInterestPeriodType posting, SavingsCompoundingInterestPeriodType compounding) {
        List<ApiParameterError> errors = new ArrayList<>();
        accountWith(posting, compounding)
                .validateInterestPostingAndCompoundingPeriodTypes(new DataValidatorBuilder(errors).resource("test"));
        return errors;
    }

    static Stream<Arguments> allAnniversaryTypesWithAllCompoundingTypes() {
        return Stream.of(Arguments.of(ANNIVERSARY_MONTHLY, DAILY), Arguments.of(ANNIVERSARY_MONTHLY, MONTHLY),
                Arguments.of(ANNIVERSARY_MONTHLY, QUATERLY), Arguments.of(ANNIVERSARY_MONTHLY, BI_ANNUAL),
                Arguments.of(ANNIVERSARY_MONTHLY, ANNUAL), Arguments.of(ANNIVERSARY_QUARTERLY, DAILY),
                Arguments.of(ANNIVERSARY_QUARTERLY, MONTHLY), Arguments.of(ANNIVERSARY_QUARTERLY, QUATERLY),
                Arguments.of(ANNIVERSARY_QUARTERLY, BI_ANNUAL), Arguments.of(ANNIVERSARY_QUARTERLY, ANNUAL),
                Arguments.of(ANNIVERSARY_BIANNUAL, DAILY), Arguments.of(ANNIVERSARY_BIANNUAL, MONTHLY),
                Arguments.of(ANNIVERSARY_BIANNUAL, QUATERLY), Arguments.of(ANNIVERSARY_BIANNUAL, BI_ANNUAL),
                Arguments.of(ANNIVERSARY_BIANNUAL, ANNUAL), Arguments.of(ANNIVERSARY_ANNUAL, DAILY),
                Arguments.of(ANNIVERSARY_ANNUAL, MONTHLY), Arguments.of(ANNIVERSARY_ANNUAL, QUATERLY),
                Arguments.of(ANNIVERSARY_ANNUAL, BI_ANNUAL), Arguments.of(ANNIVERSARY_ANNUAL, ANNUAL));
    }

    @ParameterizedTest
    @MethodSource("allAnniversaryTypesWithAllCompoundingTypes")
    void anniversaryPostingType_withAnyCompounding_shouldBeRecognizedAndProduceNoErrors(SavingsPostingInterestPeriodType posting,
            SavingsCompoundingInterestPeriodType compounding) {
        assertThat(validate(posting, compounding)).isEmpty();
    }
}

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
package org.apache.fineract.test.stepdef.loan;

import static org.apache.fineract.client.feign.util.FeignCalls.fail;
import static org.apache.fineract.client.feign.util.FeignCalls.ok;
import static org.assertj.core.api.Assertions.assertThat;

import io.cucumber.java.en.Then;
import org.apache.fineract.client.feign.FineractFeignClient;
import org.apache.fineract.client.feign.util.CallFailedRuntimeException;
import org.apache.fineract.client.models.InterestPauseRequestDto;
import org.apache.fineract.client.models.PostLoansResponse;
import org.apache.fineract.test.factory.LoanRequestFactory;
import org.apache.fineract.test.helper.ErrorMessageHelper;
import org.apache.fineract.test.stepdef.AbstractStepDef;
import org.apache.fineract.test.support.TestContextKey;
import org.springframework.beans.factory.annotation.Autowired;

public class LoanInterestPauseStepDef extends AbstractStepDef {

    @Autowired
    private FineractFeignClient fineractClient;

    @Then("Create an interest pause period with start date {string} and end date {string}")
    public void interestPauseCreate(final String startDate, final String endDate) {
        PostLoansResponse loanResponse = testContext().get(TestContextKey.LOAN_CREATE_RESPONSE);
        long loanId = loanResponse.getLoanId();

        final InterestPauseRequestDto interestPauseRequest = LoanRequestFactory.defaultInterestPauseRequest().startDate(startDate)
                .endDate(endDate);
        ok(() -> fineractClient.loanInterestPause().createInterestPause(loanId, interestPauseRequest));
    }

    @Then("Admin is not able to add an interest pause period with start date {string} and end date {string}")
    public void createInterestPauseFailure(final String startDate, final String endDate) {
        PostLoansResponse loanResponse = testContext().get(TestContextKey.LOAN_CREATE_RESPONSE);
        long loanId = loanResponse.getLoanId();

        final InterestPauseRequestDto interestPauseRequest = LoanRequestFactory.defaultInterestPauseRequest().startDate(startDate)
                .endDate(endDate);

        CallFailedRuntimeException exception = fail(
                () -> fineractClient.loanInterestPause().createInterestPause(loanId, interestPauseRequest));
        assertThat(exception.getStatus()).as(ErrorMessageHelper.addInterestPauseForNotInterestBearingLoanFailure()).isEqualTo(403);
        assertThat(exception.getDeveloperMessage()).contains(ErrorMessageHelper.addInterestPauseForNotInterestBearingLoanFailure());
    }

    @Then("Admin is not able to add an interest pause period with start date {string} and end date {string} due to inactive loan status")
    public void createInterestPauseForInactiveLoanFailure(final String startDate, final String endDate) {
        PostLoansResponse loanResponse = testContext().get(TestContextKey.LOAN_CREATE_RESPONSE);
        long loanId = loanResponse.getLoanId();

        final InterestPauseRequestDto interestPauseRequest = LoanRequestFactory.defaultInterestPauseRequest().startDate(startDate)
                .endDate(endDate);

        CallFailedRuntimeException exception = fail(
                () -> fineractClient.loanInterestPause().createInterestPause(loanId, interestPauseRequest));
        assertThat(exception.getStatus()).as(ErrorMessageHelper.addInterestPauseForNotInactiveLoanFailure()).isEqualTo(403);
        assertThat(exception.getDeveloperMessage()).contains(ErrorMessageHelper.addInterestPauseForNotInactiveLoanFailure());
    }
}

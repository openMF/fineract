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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.fineract.infrastructure.core.data.EnumOptionData;
import org.apache.fineract.portfolio.savings.SavingsPostingInterestPeriodType;
import org.junit.jupiter.api.Test;

class SavingsDropdownReadPlatformServiceImplTest {

    private final SavingsDropdownReadPlatformServiceImpl service = new SavingsDropdownReadPlatformServiceImpl();

    @Test
    void retrieveInterestPostingPeriodTypeOptions_shouldIncludeAllAnniversaryTypes() {
        List<EnumOptionData> options = service.retrieveInterestPostingPeriodTypeOptions();
        List<Long> ids = options.stream().map(EnumOptionData::getId).collect(Collectors.toList());

        assertThat(ids).contains((long) SavingsPostingInterestPeriodType.ANNIVERSARY_MONTHLY.getValue(),
                (long) SavingsPostingInterestPeriodType.ANNIVERSARY_QUARTERLY.getValue(),
                (long) SavingsPostingInterestPeriodType.ANNIVERSARY_BIANNUAL.getValue(),
                (long) SavingsPostingInterestPeriodType.ANNIVERSARY_ANNUAL.getValue());
    }

    @Test
    void retrieveInterestPostingPeriodTypeOptions_shouldIncludeAllOriginalTypes() {
        List<EnumOptionData> options = service.retrieveInterestPostingPeriodTypeOptions();
        List<Long> ids = options.stream().map(EnumOptionData::getId).collect(Collectors.toList());

        assertThat(ids).contains((long) SavingsPostingInterestPeriodType.DAILY.getValue(),
                (long) SavingsPostingInterestPeriodType.MONTHLY.getValue(), (long) SavingsPostingInterestPeriodType.QUATERLY.getValue(),
                (long) SavingsPostingInterestPeriodType.BIANNUAL.getValue(), (long) SavingsPostingInterestPeriodType.ANNUAL.getValue());
    }
}

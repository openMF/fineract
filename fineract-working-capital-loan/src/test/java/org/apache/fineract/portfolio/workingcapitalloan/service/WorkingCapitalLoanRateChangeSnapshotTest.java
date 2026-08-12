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
package org.apache.fineract.portfolio.workingcapitalloan.service;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;
import org.apache.fineract.infrastructure.businessdate.domain.BusinessDateType;
import org.apache.fineract.infrastructure.core.domain.ActionContext;
import org.apache.fineract.infrastructure.core.domain.FineractPlatformTenant;
import org.apache.fineract.infrastructure.core.service.ThreadLocalContextUtil;
import org.apache.fineract.organisation.monetary.domain.MonetaryCurrency;
import org.apache.fineract.organisation.monetary.domain.Money;
import org.apache.fineract.organisation.monetary.domain.MoneyHelper;
import org.apache.fineract.portfolio.workingcapitalloan.calc.ProjectedAmortizationScheduleModel;
import org.apache.fineract.portfolio.workingcapitalloan.calc.ProjectedAmortizationScheduleModel.RateSegment;
import org.apache.fineract.portfolio.workingcapitalloan.domain.WorkingCapitalLoanPeriodPaymentRateChange;
import org.apache.fineract.portfolio.workingcapitalloan.repository.WorkingCapitalLoanPeriodPaymentRateChangeRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

@ExtendWith(MockitoExtension.class)
class WorkingCapitalLoanRateChangeSnapshotTest {

    private static final LocalDate EFFECTIVE_DATE = LocalDate.of(2019, 1, 25);

    @Mock
    private WorkingCapitalLoanPeriodPaymentRateChangeRepository rateChangeRepository;

    @Mock
    private ProjectedAmortizationScheduleModel model;

    @InjectMocks
    private WorkingCapitalLoanWritePlatformServiceImpl service;

    @BeforeEach
    void setBusinessDate() {
        ThreadLocalContextUtil.setTenant(new FineractPlatformTenant(1L, "default", "Default", "UTC", null));
        ThreadLocalContextUtil.setActionContext(ActionContext.DEFAULT);
        ThreadLocalContextUtil.setBusinessDates(new HashMap<>(Map.of(BusinessDateType.BUSINESS_DATE, EFFECTIVE_DATE)));
        MoneyHelper.updateTenantRoundingMode("default", RoundingMode.HALF_EVEN.ordinal());
    }

    @AfterEach
    void resetContext() {
        ThreadLocalContextUtil.reset();
    }

    @Test
    void recordCalculatedValues_noSegmentOpenedOnTheEffectiveDate_leavesSnapshotUnsetAndDoesNotThrow() {
        final WorkingCapitalLoanPeriodPaymentRateChange rateChange = WorkingCapitalLoanPeriodPaymentRateChange.create(null, EFFECTIVE_DATE,
                new BigDecimal("18"), new BigDecimal("17"));
        when(model.segmentOpenedOn(any(LocalDate.class))).thenReturn(null);

        assertDoesNotThrow(() -> ReflectionTestUtils.invokeMethod(service, "recordCalculatedValues", rateChange, model));

        assertNull(rateChange.getCalculatedAnnualEir());
        assertNull(rateChange.getDailyPaymentAmount());
        assertNull(rateChange.getSegmentTerm());
        verifyNoInteractions(rateChangeRepository);
    }

    @Test
    void recordCalculatedValues_segmentOpenedOnTheEffectiveDate_storesAnnualEirAsPercentageAndSaves() {
        final WorkingCapitalLoanPeriodPaymentRateChange rateChange = WorkingCapitalLoanPeriodPaymentRateChange.create(null, EFFECTIVE_DATE,
                new BigDecimal("18"), new BigDecimal("17"));
        final MonetaryCurrency currency = new MonetaryCurrency("EUR", 2, null);
        final Money dailyPayment = Money.of(currency, new BigDecimal("47.22"), MoneyHelper.getMathContext());
        final RateSegment segment = new RateSegment(24, dailyPayment, 212, new BigDecimal("0.001008699894"),
                Money.of(currency, new BigDecimal("9000.00"), MoneyHelper.getMathContext()),
                Money.of(currency, new BigDecimal("1000.00"), MoneyHelper.getMathContext()), dailyPayment);
        when(model.segmentOpenedOn(EFFECTIVE_DATE)).thenReturn(segment);
        when(model.npvDayCount()).thenReturn(360);

        ReflectionTestUtils.invokeMethod(service, "recordCalculatedValues", rateChange, model);

        // (1 + 0.001008699894)^360 - 1 = 0.43756245..., stored as a percentage
        assertEquals(new BigDecimal("43.756245"), rateChange.getCalculatedAnnualEir().setScale(6, RoundingMode.HALF_EVEN));
        assertEquals(0, new BigDecimal("47.22").compareTo(rateChange.getDailyPaymentAmount()));
        assertEquals(212, rateChange.getSegmentTerm());
        verify(rateChangeRepository).save(rateChange);
    }
}

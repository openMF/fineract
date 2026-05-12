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
package org.apache.fineract.portfolio.tax.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.fineract.organisation.monetary.domain.MoneyHelper;
import org.apache.fineract.portfolio.tax.domain.TaxComponent;
import org.apache.fineract.portfolio.tax.domain.TaxGroup;
import org.apache.fineract.portfolio.tax.domain.TaxGroupMappings;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

/**
 * Unit tests for {@link ChargeTaxApplicationServiceImpl}.
 *
 * Each test stubs only the collaborators needed (TaxGroup, TaxGroupMappings, TaxComponent) so no Spring context or
 * database is required.
 */
class ChargeTaxApplicationServiceTest {

    private static MockedStatic<MoneyHelper> moneyHelperMock;

    private final ChargeTaxApplicationService service = new ChargeTaxApplicationServiceImpl();

    private final LocalDate actualDate = LocalDate.now(ZoneId.systemDefault());

    @BeforeAll
    static void setUpMoneyHelper() {
        moneyHelperMock = mockStatic(MoneyHelper.class);
        moneyHelperMock.when(MoneyHelper::getRoundingMode).thenReturn(RoundingMode.HALF_EVEN);
    }

    @AfterAll
    static void tearDownMoneyHelper() {
        moneyHelperMock.close();
    }

    @Test
    void computeTax_returnsEmptyMap_whenTaxGroupIsNull() {
        Map<TaxComponent, BigDecimal> result = service.computeTax(null, new BigDecimal("100.00"), actualDate, 6);

        assertThat(result).isEmpty();
    }

    @Test
    void computeTax_returnsEmptyMap_whenBaseAmountIsNull() {
        TaxGroup taxGroup = mock(TaxGroup.class);

        Map<TaxComponent, BigDecimal> result = service.computeTax(taxGroup, null, actualDate, 6);

        assertThat(result).isEmpty();
    }

    @Test
    void computeTax_returnsEmptyMap_whenBaseAmountIsZero() {
        TaxGroup taxGroup = mock(TaxGroup.class);

        Map<TaxComponent, BigDecimal> result = service.computeTax(taxGroup, BigDecimal.ZERO, actualDate, 6);

        assertThat(result).isEmpty();
    }

    @Test
    void computeTax_calculatesTaxCorrectly_forSingleComponent() {
        // base = 1000, rate = 16 % → tax = 160
        LocalDate effectiveDate = LocalDate.of(2026, 1, 1);
        TaxComponent component = taxComponentWithRate(new BigDecimal("16"), effectiveDate.minusDays(1));
        TaxGroup taxGroup = taxGroupWith(component, effectiveDate.minusDays(1), null);

        Map<TaxComponent, BigDecimal> result = service.computeTax(taxGroup, new BigDecimal("1000.00"), effectiveDate, 6);

        assertThat(result).hasSize(1);
        BigDecimal tax = result.get(component);
        assertThat(tax).isNotNull();
        assertThat(tax.setScale(2, RoundingMode.HALF_EVEN)).isEqualByComparingTo(new BigDecimal("160.00"));
    }

    @Test
    void computeTax_calculatesTaxCorrectly_forSmallAmount() {
        // base = 50, rate = 10 % → tax = 5
        LocalDate effectiveDate = LocalDate.of(2026, 3, 1);
        TaxComponent component = taxComponentWithRate(new BigDecimal("10"), effectiveDate.minusDays(1));
        TaxGroup taxGroup = taxGroupWith(component, effectiveDate.minusDays(1), null);

        Map<TaxComponent, BigDecimal> result = service.computeTax(taxGroup, new BigDecimal("50.00"), effectiveDate, 6);

        assertThat(result).hasSize(1);
        assertThat(result.get(component).setScale(2, RoundingMode.HALF_EVEN)).isEqualByComparingTo(new BigDecimal("5.00"));
    }

    @Test
    void computeTax_returnsTaxForEachActiveComponent_whenMultipleComponentsConfigured() {
        // base = 200, component1 = 10 % → 20, component2 = 5 % → 10
        LocalDate effectiveDate = LocalDate.of(2026, 4, 1);
        TaxComponent comp1 = taxComponentWithRate(new BigDecimal("10"), effectiveDate.minusDays(10));
        TaxComponent comp2 = taxComponentWithRate(new BigDecimal("5"), effectiveDate.minusDays(10));

        Set<TaxGroupMappings> mappings = new HashSet<>();
        mappings.add(activeMappingFor(comp1, effectiveDate.minusDays(10), null));
        mappings.add(activeMappingFor(comp2, effectiveDate.minusDays(10), null));

        TaxGroup taxGroup = mock(TaxGroup.class);
        when(taxGroup.getTaxGroupMappings()).thenReturn(mappings);

        Map<TaxComponent, BigDecimal> result = service.computeTax(taxGroup, new BigDecimal("200.00"), effectiveDate, 6);

        assertThat(result).hasSize(2);
        assertThat(result.get(comp1).setScale(2, RoundingMode.HALF_EVEN)).isEqualByComparingTo(new BigDecimal("20.00"));
        assertThat(result.get(comp2).setScale(2, RoundingMode.HALF_EVEN)).isEqualByComparingTo(new BigDecimal("10.00"));
    }

    @Test
    void computeTax_totalTaxSumMatchesExpected_forMultipleComponents() {
        LocalDate effectiveDate = LocalDate.of(2026, 4, 1);
        TaxComponent comp1 = taxComponentWithRate(new BigDecimal("10"), effectiveDate.minusDays(10));
        TaxComponent comp2 = taxComponentWithRate(new BigDecimal("5"), effectiveDate.minusDays(10));

        Set<TaxGroupMappings> mappings = new HashSet<>();
        mappings.add(activeMappingFor(comp1, effectiveDate.minusDays(10), null));
        mappings.add(activeMappingFor(comp2, effectiveDate.minusDays(10), null));

        TaxGroup taxGroup = mock(TaxGroup.class);
        when(taxGroup.getTaxGroupMappings()).thenReturn(mappings);

        Map<TaxComponent, BigDecimal> result = service.computeTax(taxGroup, new BigDecimal("200.00"), effectiveDate, 6);

        BigDecimal total = TaxUtils.totalTaxAmount(result);
        assertThat(total.setScale(2, RoundingMode.HALF_EVEN)).isEqualByComparingTo(new BigDecimal("30.00"));
    }

    @Test
    void computeTax_excludesExpiredComponent_whenMappingEndDateIsPast() {
        // Component mapping expired before effectiveDate → should NOT appear in result
        LocalDate effectiveDate = LocalDate.of(2026, 4, 10);
        TaxComponent component = taxComponentWithRate(new BigDecimal("15"), effectiveDate.minusDays(30));
        // mapping ended 5 days ago
        TaxGroupMappings expiredMapping = activeMappingFor(component, effectiveDate.minusDays(30), effectiveDate.minusDays(5));

        TaxGroup taxGroup = mock(TaxGroup.class);
        when(taxGroup.getTaxGroupMappings()).thenReturn(Set.of(expiredMapping));

        Map<TaxComponent, BigDecimal> result = service.computeTax(taxGroup, new BigDecimal("500.00"), effectiveDate, 6);

        assertThat(result).isEmpty();
    }

    @Test
    void computeTax_includesActiveAndExcludesExpired_whenMixedMappings() {
        LocalDate effectiveDate = LocalDate.of(2026, 4, 10);

        TaxComponent activeComp = taxComponentWithRate(new BigDecimal("10"), effectiveDate.minusDays(20));
        TaxComponent expiredComp = taxComponentWithRate(new BigDecimal("5"), effectiveDate.minusDays(20));

        TaxGroupMappings activeMapping = activeMappingFor(activeComp, effectiveDate.minusDays(20), null);
        TaxGroupMappings expiredMapping = activeMappingFor(expiredComp, effectiveDate.minusDays(20), effectiveDate.minusDays(1));

        TaxGroup taxGroup = mock(TaxGroup.class);
        when(taxGroup.getTaxGroupMappings()).thenReturn(Set.of(activeMapping, expiredMapping));

        Map<TaxComponent, BigDecimal> result = service.computeTax(taxGroup, new BigDecimal("100.00"), effectiveDate, 6);

        assertThat(result).hasSize(1).containsKey(activeComp);
        assertThat(result.get(activeComp).setScale(2, RoundingMode.HALF_EVEN)).isEqualByComparingTo(new BigDecimal("10.00"));
    }

    @Test
    void computeTax_returnsEmptyMap_whenMappingHasNotStartedYet() {
        // Component starts tomorrow → not applicable today
        LocalDate effectiveDate = LocalDate.of(2026, 4, 10);
        TaxComponent component = taxComponentWithRate(new BigDecimal("16"), effectiveDate.plusDays(1));
        TaxGroupMappings futureMapping = activeMappingFor(component, effectiveDate.plusDays(1), null);

        TaxGroup taxGroup = mock(TaxGroup.class);
        when(taxGroup.getTaxGroupMappings()).thenReturn(Set.of(futureMapping));

        Map<TaxComponent, BigDecimal> result = service.computeTax(taxGroup, new BigDecimal("1000.00"), effectiveDate, 6);

        assertThat(result).isEmpty();
    }

    @Test
    void computeTax_respectsRequestedScale() {
        LocalDate effectiveDate = LocalDate.of(2026, 4, 1);
        TaxComponent component = taxComponentWithRate(new BigDecimal("7"), effectiveDate.minusDays(1));
        TaxGroup taxGroup = taxGroupWith(component, effectiveDate.minusDays(1), null);

        Map<TaxComponent, BigDecimal> result = service.computeTax(taxGroup, new BigDecimal("333.33"), effectiveDate, 2);

        assertThat(result.get(component).scale()).isEqualTo(2);
    }

    private TaxComponent taxComponentWithRate(BigDecimal percentage, LocalDate startDate) {
        TaxComponent component = mock(TaxComponent.class);
        when(component.getApplicablePercentage(org.mockito.ArgumentMatchers.any(LocalDate.class))).thenReturn(percentage);
        return component;
    }

    private TaxGroupMappings activeMappingFor(TaxComponent component, LocalDate startDate, LocalDate endDate) {
        TaxGroupMappings mapping = mock(TaxGroupMappings.class);
        when(mapping.getTaxComponent()).thenReturn(component);
        // occursOnDayFromAndUpToAndIncluding: after startDate AND (endDate == null OR not after endDate)
        when(mapping.occursOnDayFromAndUpToAndIncluding(org.mockito.ArgumentMatchers.any(LocalDate.class))).thenAnswer(inv -> {
            LocalDate target = inv.getArgument(0);
            boolean afterStart = target.isAfter(startDate);
            boolean beforeEnd = endDate == null || !target.isAfter(endDate);
            return afterStart && beforeEnd;
        });
        return mapping;
    }

    private TaxGroup taxGroupWith(TaxComponent component, LocalDate startDate, LocalDate endDate) {
        TaxGroupMappings mapping = activeMappingFor(component, startDate, endDate);
        TaxGroup taxGroup = mock(TaxGroup.class);
        when(taxGroup.getTaxGroupMappings()).thenReturn(Set.of(mapping));
        return taxGroup;
    }
}

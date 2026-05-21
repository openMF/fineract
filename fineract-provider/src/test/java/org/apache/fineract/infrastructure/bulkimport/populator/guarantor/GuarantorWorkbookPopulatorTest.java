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
package org.apache.fineract.infrastructure.bulkimport.populator.guarantor;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import java.util.ArrayList;
import java.util.List;
import org.apache.fineract.infrastructure.bulkimport.populator.ClientSheetPopulator;
import org.apache.fineract.infrastructure.bulkimport.populator.OfficeSheetPopulator;
import org.apache.fineract.portfolio.loanaccount.data.LoanAccountData;
import org.apache.fineract.portfolio.loanaccount.data.LoanStatusEnumData;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Workbook;
import org.junit.jupiter.api.Test;

class GuarantorWorkbookPopulatorTest {

    private static final LoanStatusEnumData ACTIVE_STATUS = new LoanStatusEnumData(300L, "loanStatusType.active", "Active");
    private static final String DATE_FORMAT = "dd MMMM yyyy";

    private LoanAccountData loanWithAccountNo(String accountNo) {
        return new LoanAccountData().setAccountNo(accountNo).setStatus(ACTIVE_STATUS).setClientName("John Doe").setClientId(1L);
    }

    private GuarantorWorkbookPopulator populatorFor(LoanAccountData loan) {
        return new GuarantorWorkbookPopulator(new OfficeSheetPopulator(List.of()), new ClientSheetPopulator(List.of(), List.of()),
                new ArrayList<>(List.of(loan)), new ArrayList<>(), List.of());
    }

    @Test
    void populate_withAlphanumericAccountNumber() {
        try (Workbook workbook = new HSSFWorkbook()) {
            assertDoesNotThrow(() -> populatorFor(loanWithAccountNo("ABC-001")).populate(workbook, DATE_FORMAT));
        } catch (Exception e) {
            // ignore workbook close exceptions
        }
    }

    @Test
    void populate_withMixedAccountNumber() {
        try (Workbook workbook = new HSSFWorkbook()) {
            assertDoesNotThrow(() -> populatorFor(loanWithAccountNo("001ABC")).populate(workbook, DATE_FORMAT));
        } catch (Exception e) {
            // ignore workbook close exceptions
        }
    }

    @Test
    void populate_withNumericAccountNumber() {
        try (Workbook workbook = new HSSFWorkbook()) {
            assertDoesNotThrow(() -> populatorFor(loanWithAccountNo("000001")).populate(workbook, DATE_FORMAT));
        } catch (Exception e) {
            // ignore workbook close exceptions
        }
    }
}

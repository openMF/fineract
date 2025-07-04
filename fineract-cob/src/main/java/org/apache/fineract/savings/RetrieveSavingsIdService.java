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
package org.apache.fineract.cob.savings;

import java.time.LocalDate;
import java.util.List;
import org.apache.fineract.cob.data.AccountCOBParameter;
import org.apache.fineract.cob.data.AccountCOBPartition;
import org.apache.fineract.cob.data.AccountIdAndExternalIdAndAccountNo;
import org.apache.fineract.cob.data.AccountIdAndLastClosedBusinessDate;
import org.springframework.data.repository.query.Param;

public interface RetrieveSavingsIdService {

    List<AccountCOBPartition> retrieveSavingsCOBPartitions(Long numberOfDays, LocalDate businessDate, boolean isCatchUp, int partitionSize);

    List<AccountIdAndLastClosedBusinessDate> retrieveSavingsIdsBehindDate(LocalDate businessDate, List<Long> savingsIds);

    List<AccountIdAndLastClosedBusinessDate> retrieveSavingsIdsBehindDateOrNull(LocalDate businessDate, List<Long> savingsIds);

    List<AccountIdAndLastClosedBusinessDate> retrieveSavingsIdsOldestCobProcessed(LocalDate businessDate);

    List<Long> retrieveAllNonClosedSavingsByLastClosedBusinessDateAndMinAndMaxSavingsId(AccountCOBParameter savingsCOBParameter,
            boolean isCatchUp);

    List<AccountIdAndExternalIdAndAccountNo> findAllStayedLockedByCobBusinessDate(@Param("cobBusinessDate") LocalDate cobBusinessDate);

}

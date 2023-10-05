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

import jakarta.persistence.LockModeType;
import java.time.LocalDate;
import java.util.List;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface SavingsAccountTransactionRepository
        extends JpaRepository<SavingsAccountTransaction, Long>, JpaSpecificationExecutor<SavingsAccountTransaction> {

    @Query("select sat.id from SavingsAccountTransaction sat where sat.savingsAccount.id = :savingAccountId")
    List<Long> findAllTransactionIds(@Param("savingAccountId") Long savingAccountId);

    @Query("select sat.id from SavingsAccountTransaction sat where sat.savingsAccount.id = :savingAccountId and sat.reversed = :reversed")
    List<Long> findAllTransactionIds(@Param("savingAccountId") Long savingAccountId, @Param("reversed") boolean reversed);

    @Query("select sat from SavingsAccountTransaction sat where sat.id = :transactionId and sat.savingsAccount.id = :savingsId")
    SavingsAccountTransaction findOneByIdAndSavingsAccountId(@Param("transactionId") Long transactionId,
            @Param("savingsId") Long savingsId);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select st from SavingsAccountTransaction st where st.savingsAccount = :savingsAccount and st.dateOf >= :transactionDate order by st.dateOf,st.createdDate,st.id")
    List<SavingsAccountTransaction> findTransactionsAfterPivotDate(@Param("savingsAccount") SavingsAccount savingsAccount,
            @Param("transactionDate") LocalDate transactionDate);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select st from SavingsAccountTransaction st where st.savingsAccount = :savingsAccount and st.dateOf = :date and st.reversalTransaction <> 1 and st.reversed <> 1 order by st.id")
    List<SavingsAccountTransaction> findTransactionRunningBalanceBeforePivotDate(@Param("savingsAccount") SavingsAccount savingsAccount,
            @Param("date") LocalDate date);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    List<SavingsAccountTransaction> findBySavingsAccount(@Param("savingsAccount") SavingsAccount savingsAccount);

    List<SavingsAccountTransaction> findByRefNo(@Param("refNo") String refNo);

    @Query("select sat from SavingsAccountTransaction sat where sat.savingsAccount.id = :savingsId and sat.dateOf <= :transactionDate and sat.reversed=false")
    List<SavingsAccountTransaction> findBySavingsAccountIdAndLessThanDateOfAndReversedIsFalse(@Param("savingsId") Long savingsId,
            @Param("transactionDate") LocalDate transactionDate, Pageable pageable);

    // trans.isInterestPosting() && trans.isNotReversed() && !trans.isReversalTransaction() && trans.isManualTransaction()
    // (transactionType = (INTEREST_POSTING || OVERDRAFT_INTEREST)) & !reversed & !reversalTransaction & isManualTransaction
    @Query("select sat.dateOf from SavingsAccountTransaction sat where sat.savingsAccount.id = :savingId and sat.typeOf IN :transactionTypes and sat.reversed = :reversed and sat.reversalTransaction = :reversalTransaction and sat.isManualTransaction = :isManualTransaction")
    List<LocalDate> findAllManualPostingDates(@Param("savingId") Long accountId, @Param("transactionTypes") List<Integer> transactionTypes, @Param("reversed") boolean reserved, @Param("reversalTransaction") boolean reversalTransaction, @Param("isManualTransaction") boolean isManualTransaction);
}

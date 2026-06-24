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
package org.apache.fineract.infrastructure.event.business.service;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * Guards the connection-leak fix: {@link TransactionHelper#hasTransaction()} must reflect whether an <em>actual</em>
 * (connection-backed) transaction is active, so that {@code notifyPostBusinessEvent} only defers events to commit when
 * a real transaction exists. The previous implementation relied on {@code TransactionAspectSupport} together with a
 * {@code @Transactional(SUPPORTS)} proxy, which established a synchronization scope that held a DB connection
 * idle-in-transaction for the whole request.
 */
class TransactionHelperTest {

    private final TransactionHelper underTest = new TransactionHelper();

    @AfterEach
    void tearDown() {
        // Make sure the thread-bound flag never leaks into other tests.
        if (TransactionSynchronizationManager.isActualTransactionActive()) {
            TransactionSynchronizationManager.setActualTransactionActive(false);
        }
    }

    @Test
    void shouldReportNoTransactionWhenNoActualTransactionIsActive() {
        TransactionSynchronizationManager.setActualTransactionActive(false);

        assertThat(underTest.hasTransaction()).isFalse();
    }

    @Test
    void shouldReportTransactionWhenAnActualTransactionIsActive() {
        TransactionSynchronizationManager.setActualTransactionActive(true);

        assertThat(underTest.hasTransaction()).isTrue();
    }
}

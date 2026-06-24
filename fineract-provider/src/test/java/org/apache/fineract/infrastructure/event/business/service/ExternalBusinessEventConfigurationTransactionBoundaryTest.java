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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.fineract.infrastructure.core.config.FineractProperties;
import org.apache.fineract.infrastructure.event.business.domain.BusinessEvent;
import org.apache.fineract.infrastructure.event.external.repository.ExternalEventConfigurationRepository;
import org.apache.fineract.infrastructure.event.external.repository.domain.ExternalEventConfiguration;
import org.apache.fineract.infrastructure.event.external.service.ExternalEventService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.TransactionSynchronizationManager;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = ExternalBusinessEventConfigurationTransactionBoundaryTest.TestConfig.class)
@SuppressWarnings("unchecked")
class ExternalBusinessEventConfigurationTransactionBoundaryTest {

    @Autowired
    private BusinessEventNotifierServiceImpl businessEventNotifierService;

    @Autowired
    private TransactionBoundaryProbe transactionBoundaryProbe;

    @Autowired
    private ProbeTransactionManager transactionManager;

    @AfterEach
    void tearDown() {
        transactionBoundaryProbe.releasePostEvent();
    }

    @Test
    void configurationLookupTransactionMustCloseBeforeDownstreamEventPostingBlocks() throws Exception {
        BusinessEvent<?> businessEvent = mock(BusinessEvent.class);
        given(businessEvent.getType()).willReturn("LoanApprovedBusinessEvent");

        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<?> future = executor.submit(() -> businessEventNotifierService.notifyPostBusinessEvent(businessEvent));
        try {
            assertThat(transactionBoundaryProbe.awaitPostEvent()).as("postEvent should be reached").isTrue();

            assertThat(transactionBoundaryProbe.isTransactionActiveDuringConfigurationLookup())
                    .as("configuration lookup should run inside a real transaction").isTrue();
            assertThat(transactionBoundaryProbe.isTransactionActiveAtPostEventEntry())
                    .as("configuration lookup transaction must be closed before downstream posting starts").isFalse();
            assertThat(transactionManager.getBeginCount()).as("configuration lookup should start one transaction").isEqualTo(1);
            assertThat(transactionManager.getCommitCount()).as("configuration lookup transaction should already be committed").isEqualTo(1);
            assertThat(transactionManager.getRollbackCount()).as("configuration lookup should not roll back").isZero();

            transactionBoundaryProbe.releasePostEvent();
            future.get(5, TimeUnit.SECONDS);
        } finally {
            transactionBoundaryProbe.releasePostEvent();
            executor.shutdownNow();
        }
    }

    @Configuration
    @EnableTransactionManagement
    static class TestConfig {

        @Bean
        ProbeTransactionManager transactionManager() {
            return new ProbeTransactionManager();
        }

        @Bean
        TransactionBoundaryProbe transactionBoundaryProbe() {
            return new TransactionBoundaryProbe();
        }

        @Bean
        ExternalEventConfigurationRepository externalEventConfigurationRepository(TransactionBoundaryProbe transactionBoundaryProbe) {
            ExternalEventConfigurationRepository repository = mock(ExternalEventConfigurationRepository.class);
            given(repository.findExternalEventConfigurationByTypeWithNotFoundDetection(anyString())).willAnswer(invocation -> {
                transactionBoundaryProbe.recordConfigurationLookupTransactionState();
                return new ExternalEventConfiguration(invocation.getArgument(0), true);
            });
            return repository;
        }

        @Bean
        ExternalBusinessEventConfigurationService externalBusinessEventConfigurationService(
                ExternalEventConfigurationRepository externalEventConfigurationRepository) {
            return new ExternalBusinessEventConfigurationServiceImpl(externalEventConfigurationRepository);
        }

        @Bean
        FineractProperties fineractProperties() {
            FineractProperties fineractProperties = new FineractProperties();
            FineractProperties.FineractEventsProperties eventsProperties = new FineractProperties.FineractEventsProperties();
            FineractProperties.FineractExternalEventsProperties externalEventsProperties = new FineractProperties.FineractExternalEventsProperties();
            externalEventsProperties.setEnabled(true);
            eventsProperties.setExternal(externalEventsProperties);
            fineractProperties.setEvents(eventsProperties);
            return fineractProperties;
        }

        @Bean
        TransactionHelper transactionHelper() {
            return new TransactionHelper();
        }

        @Bean
        BusinessEventNotifierServiceImpl businessEventNotifierService(FineractProperties fineractProperties,
                TransactionHelper transactionHelper, ExternalBusinessEventConfigurationService externalBusinessEventConfigurationService,
                TransactionBoundaryProbe transactionBoundaryProbe) {
            ExternalEventService externalEventService = mock(ExternalEventService.class);
            doAnswer(invocation -> {
                transactionBoundaryProbe.recordPostEventEntryTransactionState();
                assertThat(transactionBoundaryProbe.awaitReleasePostEvent()).as("postEvent should be released").isTrue();
                return null;
            }).when(externalEventService).postEvent(any(BusinessEvent.class));
            return new BusinessEventNotifierServiceImpl(externalEventService, fineractProperties, transactionHelper,
                    externalBusinessEventConfigurationService);
        }
    }

    static final class TransactionBoundaryProbe {

        private final CountDownLatch postEventEntered = new CountDownLatch(1);
        private final CountDownLatch releasePostEvent = new CountDownLatch(1);
        private final AtomicBoolean transactionActiveDuringConfigurationLookup = new AtomicBoolean();
        private final AtomicBoolean transactionActiveAtPostEventEntry = new AtomicBoolean();

        void recordConfigurationLookupTransactionState() {
            transactionActiveDuringConfigurationLookup.set(TransactionSynchronizationManager.isActualTransactionActive());
        }

        void recordPostEventEntryTransactionState() {
            transactionActiveAtPostEventEntry.set(TransactionSynchronizationManager.isActualTransactionActive());
            postEventEntered.countDown();
        }

        boolean awaitPostEvent() throws InterruptedException {
            return postEventEntered.await(5, TimeUnit.SECONDS);
        }

        boolean awaitReleasePostEvent() throws InterruptedException {
            return releasePostEvent.await(5, TimeUnit.SECONDS);
        }

        void releasePostEvent() {
            releasePostEvent.countDown();
        }

        boolean isTransactionActiveDuringConfigurationLookup() {
            return transactionActiveDuringConfigurationLookup.get();
        }

        boolean isTransactionActiveAtPostEventEntry() {
            return transactionActiveAtPostEventEntry.get();
        }
    }

    static final class ProbeTransactionManager extends AbstractPlatformTransactionManager {

        private final AtomicInteger beginCount = new AtomicInteger();
        private final AtomicInteger commitCount = new AtomicInteger();
        private final AtomicInteger rollbackCount = new AtomicInteger();

        @Override
        protected Object doGetTransaction() {
            return new Object();
        }

        @Override
        protected void doBegin(Object transaction, TransactionDefinition definition) {
            beginCount.incrementAndGet();
        }

        @Override
        protected void doCommit(DefaultTransactionStatus status) {
            commitCount.incrementAndGet();
        }

        @Override
        protected void doRollback(DefaultTransactionStatus status) {
            rollbackCount.incrementAndGet();
        }

        int getBeginCount() {
            return beginCount.get();
        }

        int getCommitCount() {
            return commitCount.get();
        }

        int getRollbackCount() {
            return rollbackCount.get();
        }
    }
}

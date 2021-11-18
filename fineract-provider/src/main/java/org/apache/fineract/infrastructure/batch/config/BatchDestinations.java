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
package org.apache.fineract.infrastructure.batch.config;

import java.util.Properties;

import org.springframework.core.env.Environment;

public class BatchDestinations {

    private Properties batchJobProperties;
    private Environment environment;

    public BatchDestinations(Properties batchJobProperties, Environment environment) {
        this.batchJobProperties = batchJobProperties;
        this.environment = environment;
    }

    public String getConcurrencyDestination() {
        String value = environment.getProperty("FINERACT_BATCH_LISTENER_CONCURRENCY");
        if (value != null)
            return value;
        value = batchJobProperties.getProperty("fineract.batch.jobs.listeners.concurrency");
        if (value != null)
            return value;
        return "1-5";
    }

    public String getBatchLoansDestination() {
        String value = environment.getProperty("FINERACT_BATCH_QUEUES_BATCHLOANS");
        if (value != null)
            return value;
        value =  batchJobProperties.getProperty("fineract.batch.jobs.queues.batchLoans");
        if (value != null)
            return value;
        return "BATCH_LOANS_RQT";
    }

    public String getApplyChargeToOverdueLoanDestination() {
        return batchJobProperties.getProperty("fineract.batch.jobs.queues.applyChargeToOverdueLoans");
    }
}

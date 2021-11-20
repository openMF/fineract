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

public class BatchConstants {

    public static final String BATCH_PROPERTIES_FILE = "batch.yml";

    public static final String BATCH_BLOCKLOANS_JOB_NAME = "blockLoansJob";
    public static final String BATCH_COB_JOB_NAME = "closeOfBusinessJob";

    public static final String JOB_PARAM_PARAMETER = "parameter";
    public static final String JOB_PARAM_TENANT_ID = "tenantIdentifier";
    public static final String JOB_PARAM_TENANT_DATE = "tenantDate";

    public static final String DEFAULT_BATCH_DATE_FORMAT = "yyyy-MM-dd";
    public static final String DEFAULT_BATCH_DATE_LOCALE = "en";

    public static final String JOB_PARAM_PENALTY_WAIT_PERIOD = "penaltyWaitPeriod";
    public static final String JOB_PARAM_BACKDATE_PENALTIES = "backdatePenalties";

    public static final Long BATCH_JOB_PROCESS_ID = 2L;
}

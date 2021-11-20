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

package org.apache.fineract.infrastructure.jobs.data;

public class JobConstants {

    // Spring Profiles
    public static final String BASIC_AUTH_PROFILE_NAME = "basicauth";
    public static final String TWOFACT_AUTH_PROFILE_NAME = "twofactor";
    public static final String QUARTZ_BATCH_PROFILE_NAME = "quartzJobs";
    public static final String SPRING_BATCH_PROFILE_NAME = "springBatch";
    public static final String SPRING_BATCH_WORKER_PROFILE_NAME = "springBatchWorker";

    public static final String SPRING_MESSAGING_PROFILE_NAME = "messaging";
    public static final String SPRING_MESSAGINGSQS_PROFILE_NAME = "messagingSQS";

    public static final String SPRING_KAFKA_PROFILE_NAME = "kafkaEnabled";

}

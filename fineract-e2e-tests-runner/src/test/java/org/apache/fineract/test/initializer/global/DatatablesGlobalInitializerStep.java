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
package org.apache.fineract.test.initializer.global;

import static org.apache.fineract.client.feign.util.FeignCalls.executeVoid;

import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.fineract.client.feign.FineractFeignClient;
import org.apache.fineract.client.models.PostColumnHeaderData;
import org.apache.fineract.client.models.PostDataTablesRequest;
import org.apache.fineract.test.helper.ParallelExecutionHelper;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

@Slf4j
@RequiredArgsConstructor
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class DatatablesGlobalInitializerStep implements FineractGlobalInitializerStep {

    public static final String DATA_TABLE_1_NAME = "dt_autopay_details";
    public static final String DATA_TABLE_1_APP_NAME = "m_loan";
    public static final String DATA_TABLE_1_COLUMN_1_NAME = "financial_instruments";
    public static final String DATA_TABLE_1_COLUMN_1_TYPE = "Dropdown";
    public static final String DATA_TABLE_1_COLUMN_1_CODE = "financial_instrument";
    public static final String DATA_TABLE_1_COLUMN_2_NAME = "date_of_payment";
    public static final String DATA_TABLE_1_COLUMN_2_TYPE = "Date";
    public static final String DATA_TABLE_2_NAME = "dt_schedule_payments";
    public static final String DATA_TABLE_2_APP_NAME = "m_loan";
    public static final String DATA_TABLE_2_COLUMN_1_NAME = "external_reference_id";
    public static final String DATA_TABLE_2_COLUMN_1_TYPE = "String";
    public static final Long DATA_TABLE_2_COLUMN_1_LENGTH = 50L;
    public static final String DATA_TABLE_2_COLUMN_2_NAME = "scheduled_date";
    public static final String DATA_TABLE_2_COLUMN_2_TYPE = "Date";
    public static final String DATA_TABLE_2_COLUMN_3_NAME = "amount";
    public static final String DATA_TABLE_2_COLUMN_3_TYPE = "Decimal";
    public static final String DATA_TABLE_2_COLUMN_4_NAME = "transaction_type";
    public static final String DATA_TABLE_2_COLUMN_4_TYPE = "Dropdown";
    public static final String DATA_TABLE_2_COLUMN_4_CODE = "transaction_type";
    public static final String DATA_TABLE_3_NAME = "dt_user_tags";
    public static final String DATA_TABLE_3_APP_NAME = "m_client";
    public static final String DATA_TABLE_3_ENTITY_SUBTYPE = "PERSON";
    public static final String DATA_TABLE_3_COLUMN_1_NAME = "bankruptcy_tag";
    public static final String DATA_TABLE_3_COLUMN_1_TYPE = "Dropdown";
    public static final String DATA_TABLE_3_COLUMN_1_CODE = "bankruptcy_tag";
    public static final String DATA_TABLE_3_COLUMN_2_NAME = "pending_fraud_tag";
    public static final String DATA_TABLE_3_COLUMN_2_TYPE = "Dropdown";
    public static final String DATA_TABLE_3_COLUMN_2_CODE = "pending_fraud_tag";
    public static final String DATA_TABLE_3_COLUMN_3_NAME = "pending_deceased_tag";
    public static final String DATA_TABLE_3_COLUMN_3_TYPE = "Dropdown";
    public static final String DATA_TABLE_3_COLUMN_3_CODE = "pending_deceased_tag";
    public static final String DATA_TABLE_3_COLUMN_4_NAME = "hardship_tag";
    public static final String DATA_TABLE_3_COLUMN_4_TYPE = "Dropdown";
    public static final String DATA_TABLE_3_COLUMN_4_CODE = "hardship_tag";
    public static final String DATA_TABLE_3_COLUMN_5_NAME = "active_duty_tag";
    public static final String DATA_TABLE_3_COLUMN_5_TYPE = "Dropdown";
    public static final String DATA_TABLE_3_COLUMN_5_CODE = "active_duty_tag";

    private final FineractFeignClient fineractClient;

    @Override
    public void initialize() {
        List<Runnable> items = List.of(() -> createDatatableIdempotent(new PostDataTablesRequest().datatableName(DATA_TABLE_1_NAME)
                .apptableName(DATA_TABLE_1_APP_NAME).multiRow(true)
                .columns(List.of(
                        new PostColumnHeaderData().name(DATA_TABLE_1_COLUMN_1_NAME).type(DATA_TABLE_1_COLUMN_1_TYPE)
                                .code(DATA_TABLE_1_COLUMN_1_CODE).mandatory(false),
                        new PostColumnHeaderData().name(DATA_TABLE_1_COLUMN_2_NAME).type(DATA_TABLE_1_COLUMN_2_TYPE).mandatory(false)))),
                () -> createDatatableIdempotent(
                        new PostDataTablesRequest().datatableName(DATA_TABLE_2_NAME).apptableName(DATA_TABLE_2_APP_NAME).multiRow(true)
                                .columns(List.of(
                                        new PostColumnHeaderData().name(DATA_TABLE_2_COLUMN_1_NAME).type(DATA_TABLE_2_COLUMN_1_TYPE)
                                                .length(DATA_TABLE_2_COLUMN_1_LENGTH).mandatory(false),
                                        new PostColumnHeaderData().name(DATA_TABLE_2_COLUMN_2_NAME).type(DATA_TABLE_2_COLUMN_2_TYPE)
                                                .mandatory(false),
                                        new PostColumnHeaderData().name(DATA_TABLE_2_COLUMN_3_NAME).type(DATA_TABLE_2_COLUMN_3_TYPE)
                                                .mandatory(false),
                                        new PostColumnHeaderData().name(DATA_TABLE_2_COLUMN_4_NAME).type(DATA_TABLE_2_COLUMN_4_TYPE)
                                                .code(DATA_TABLE_2_COLUMN_4_CODE).mandatory(false)))),
                () -> createDatatableIdempotent(new PostDataTablesRequest().datatableName(DATA_TABLE_3_NAME)
                        .apptableName(DATA_TABLE_3_APP_NAME).entitySubType(DATA_TABLE_3_ENTITY_SUBTYPE).multiRow(false)
                        .columns(List.of(
                                new PostColumnHeaderData().name(DATA_TABLE_3_COLUMN_1_NAME).type(DATA_TABLE_3_COLUMN_1_TYPE)
                                        .code(DATA_TABLE_3_COLUMN_1_CODE).mandatory(false),
                                new PostColumnHeaderData().name(DATA_TABLE_3_COLUMN_2_NAME).type(DATA_TABLE_3_COLUMN_2_TYPE)
                                        .code(DATA_TABLE_3_COLUMN_2_CODE).mandatory(false),
                                new PostColumnHeaderData().name(DATA_TABLE_3_COLUMN_3_NAME).type(DATA_TABLE_3_COLUMN_3_TYPE)
                                        .code(DATA_TABLE_3_COLUMN_3_CODE).mandatory(false),
                                new PostColumnHeaderData().name(DATA_TABLE_3_COLUMN_4_NAME).type(DATA_TABLE_3_COLUMN_4_TYPE)
                                        .code(DATA_TABLE_3_COLUMN_4_CODE).mandatory(false),
                                new PostColumnHeaderData().name(DATA_TABLE_3_COLUMN_5_NAME).type(DATA_TABLE_3_COLUMN_5_TYPE)
                                        .code(DATA_TABLE_3_COLUMN_5_CODE).mandatory(false)))));
        ParallelExecutionHelper.runInParallel(items);
    }

    private void createDatatableIdempotent(PostDataTablesRequest datatableRequest) {
        String datatableName = datatableRequest.getDatatableName();
        try {
            fineractClient.dataTables().getDatatable(datatableName, Map.of());
        } catch (Exception e) {
            log.debug("Datatable '{}' does not exist yet, will create it", datatableName);
            executeVoid(() -> fineractClient.dataTables().createDatatable(datatableRequest, Map.of()));
        }
    }
}

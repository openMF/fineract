--
-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements. See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership. The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License. You may obtain a copy of the License at
--
-- http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied. See the License for the
-- specific language governing permissions and limitations
-- under the License.
--
ALTER TABLE job CHANGE COLUMN create_time create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE job ALTER COLUMN create_time DROP DEFAULT;

ALTER TABLE job_run_history CHANGE COLUMN start_time start_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE job_run_history ALTER COLUMN start_time DROP DEFAULT;

ALTER TABLE m_floating_rates CHANGE COLUMN created_date created_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE m_floating_rates ALTER COLUMN created_date DROP DEFAULT;

ALTER TABLE m_floating_rates_periods CHANGE COLUMN from_date from_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE m_floating_rates_periods ALTER COLUMN from_date DROP DEFAULT;

ALTER TABLE m_holiday CHANGE COLUMN from_date from_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE m_holiday ALTER COLUMN from_date DROP DEFAULT;

ALTER TABLE m_import_document CHANGE COLUMN import_time import_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE m_import_document ALTER COLUMN import_time DROP DEFAULT;

ALTER TABLE m_portfolio_command_source CHANGE COLUMN made_on_date made_on_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE m_portfolio_command_source ALTER COLUMN made_on_date DROP DEFAULT;

ALTER TABLE m_tax_component CHANGE COLUMN created_date created_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE m_tax_component ALTER COLUMN created_date DROP DEFAULT;

ALTER TABLE m_tax_component_history CHANGE COLUMN created_date created_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE m_tax_component_history ALTER COLUMN created_date DROP DEFAULT;

ALTER TABLE m_tax_group CHANGE COLUMN created_date created_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE m_tax_group ALTER COLUMN created_date DROP DEFAULT;

ALTER TABLE client_device_registration CHANGE COLUMN updatedon_date updatedon_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE client_device_registration ALTER COLUMN updatedon_date DROP DEFAULT;

ALTER TABLE m_client_transaction CHANGE COLUMN created_date created_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE m_client_transaction ALTER COLUMN created_date DROP DEFAULT;

ALTER TABLE interop_identifier CHANGE COLUMN created_on created_on TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE interop_identifier ALTER COLUMN created_on DROP DEFAULT;

ALTER TABLE m_deposit_account_on_hold_transaction CHANGE COLUMN created_date created_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE m_deposit_account_on_hold_transaction ALTER COLUMN created_date DROP DEFAULT;

ALTER TABLE m_savings_account_transaction CHANGE COLUMN created_date created_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE m_savings_account_transaction ALTER COLUMN created_date DROP DEFAULT;

ALTER TABLE m_tax_group_mappings CHANGE COLUMN created_date created_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE m_tax_group_mappings ALTER COLUMN created_date DROP DEFAULT;

ALTER TABLE m_cashier_transactions CHANGE COLUMN created_date created_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE m_cashier_transactions ALTER COLUMN created_date DROP DEFAULT;

ALTER TABLE m_account_transfer_standing_instructions_history CHANGE COLUMN execution_time execution_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE m_account_transfer_standing_instructions_history ALTER COLUMN execution_time DROP DEFAULT;

ALTER TABLE m_loan_disbursement_detail CHANGE COLUMN expected_disburse_date expected_disburse_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE m_loan_disbursement_detail ALTER COLUMN expected_disburse_date DROP DEFAULT;

ALTER TABLE acc_gl_journal_entry CHANGE COLUMN created_date created_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE acc_gl_journal_entry ALTER COLUMN created_date DROP DEFAULT;

ALTER TABLE m_report_mailing_job CHANGE COLUMN start_datetime start_datetime TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE m_report_mailing_job ALTER COLUMN start_datetime DROP DEFAULT;

ALTER TABLE m_report_mailing_job_run_history CHANGE COLUMN start_datetime start_datetime TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE m_report_mailing_job_run_history ALTER COLUMN start_datetime DROP DEFAULT;

ALTER TABLE twofactor_access_token CHANGE COLUMN valid_from valid_from TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE twofactor_access_token ALTER COLUMN valid_from DROP DEFAULT;
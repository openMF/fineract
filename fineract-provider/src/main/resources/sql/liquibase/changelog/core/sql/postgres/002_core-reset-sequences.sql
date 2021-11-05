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
SELECT SETVAL('c_configuration_id_seq', COALESCE(MAX(id), 0)+1, false ) FROM c_configuration;
SELECT SETVAL('job_id_seq', COALESCE(MAX(id), 0)+1, false ) FROM job;
SELECT SETVAL('m_appuser_id_seq', COALESCE(MAX(id), 0)+1, false ) FROM m_appuser;
SELECT SETVAL('m_code_id_seq', COALESCE(MAX(id), 0)+1, false ) FROM m_code;
SELECT SETVAL('m_role_id_seq', COALESCE(MAX(id), 0)+1, false ) FROM m_role;
SELECT SETVAL('m_permission_id_seq', COALESCE(MAX(id), 0)+1, false ) FROM m_permission;
SELECT SETVAL('m_hook_templates_id_seq', COALESCE(MAX(id), 0)+1, false ) FROM m_hook_templates;
SELECT SETVAL('stretchy_parameter_id_seq', COALESCE(MAX(id), 0)+1, false ) FROM stretchy_parameter;
SELECT SETVAL('stretchy_report_id_seq', COALESCE(MAX(id), 0)+1, false ) FROM stretchy_report;
SELECT SETVAL('stretchy_report_parameter_id_seq', COALESCE(MAX(id), 0)+1, false ) FROM stretchy_report_parameter;

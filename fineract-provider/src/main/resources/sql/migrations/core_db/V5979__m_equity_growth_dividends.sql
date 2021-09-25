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
-- Created 23/07/2021 

CREATE TABLE `m_equity_growth_dividends` (
	`id` BIGINT(20) NOT NULL AUTO_INCREMENT,
	`start_period` DATE NOT NULL,
	`end_period` DATE NOT NULL,
	`total_profits` DECIMAL(20,6) NOT NULL,
	`beneficiaries` INT(20) NOT NULL,
	`savings_product_id` BIGINT(20) NOT NULL,
	PRIMARY KEY (`id`) USING BTREE
)
COLLATE='latin1_swedish_ci'
ENGINE=InnoDB
;
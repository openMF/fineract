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
package custom.fineract.migration.loan.service;

import java.util.Optional;

/**
 * Interface for migration service operations.
 */
public interface MigrationService {

    /**
     * Finds the last migration status for a given loan ID.
     *
     * @param loanId
     *            the ID of the loan
     * @return Optional containing the status string if found, empty Optional otherwise
     */
    Optional<String> findLastStatus(Long loanId);
}

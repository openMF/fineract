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
package org.apache.fineract.infrastructure.core.persistence.visibility;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/**
 * Minimal entity used solely by {@link JpaJdbcSameTransactionVisibilityTest}. Lives in its own package so the test's
 * {@code setPackagesToScan(...)} discovers exactly this entity and nothing else. The id is assigned explicitly (no
 * generation strategy) to keep the persist/flush behaviour deterministic and database-agnostic.
 */
@Entity
@Table(name = "visibility_probe")
public class VisibilityProbe {

    @Id
    @Column(name = "id")
    private Long id;

    @Column(name = "name")
    private String name;

    public VisibilityProbe() {}

    public VisibilityProbe(Long id, String name) {
        this.id = id;
        this.name = name;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}

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
package org.apache.fineract.infrastructure.hooks.domain;

import jakarta.persistence.CascadeType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.OneToMany;
import jakarta.persistence.Table;
import java.util.HashSet;
import java.util.Set;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.fineract.infrastructure.core.domain.AbstractAuditableCustom;
import org.apache.fineract.template.domain.Template;

@Entity
@Table(name = "m_hook")
@Getter
@Setter
@NoArgsConstructor
@Accessors(chain = true)
public final class Hook extends AbstractAuditableCustom {

    @Column(name = "name", nullable = false, length = 100)
    private String name;

    @Column(name = "is_active", nullable = false)
    private Boolean isActive;

    @OneToMany(cascade = CascadeType.ALL, mappedBy = "hook", orphanRemoval = true, fetch = FetchType.EAGER)
    private Set<HookResource> events = new HashSet<>();

    @OneToMany(cascade = CascadeType.ALL, mappedBy = "hook", orphanRemoval = true, fetch = FetchType.EAGER)
    private Set<HookConfiguration> config = new HashSet<>();

    @ManyToOne(optional = true)
    @JoinColumn(name = "template_id", referencedColumnName = "id", nullable = false)
    private HookTemplate template;

    @ManyToOne(optional = true)
    @JoinColumn(name = "ugd_template_id", referencedColumnName = "id", nullable = true)
    private Template ugdTemplate;
}

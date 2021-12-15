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
package org.apache.fineract.infrastructure.jpa;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.persistence.Entity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.orm.jpa.persistenceunit.MutablePersistenceUnitInfo;
import org.springframework.orm.jpa.persistenceunit.PersistenceUnitPostProcessor;

/**
 * This PersistenceUnitPostProcessor is used to search given package list for JPA
 * entities and add them as managed entities. By default the JPA engine searches
 * for persistent classes only in the same class-path of the location of the
 * persistence.xml file.  When running unit tests the entities end up in test-classes
 * folder which does not get scanned.  To avoid specifying each entity in the persistence.xml
 * file to scan, this post processor automatically adds the entities for you.
 *
 */
public class CustomPersistenceUnitPostProcessor implements PersistenceUnitPostProcessor, InitializingBean {

    private static final Logger log = LoggerFactory.getLogger(CustomPersistenceUnitPostProcessor.class);

    /** the path of packages to search for persistent classes (e.g. org.springframework). Subpackages will be visited, too */
    private List<String> packages;

    /** the calculated list of additional persistent classes */
    private Set<Class<? extends Object>> persistentClasses;

    /**
     * Looks for any persistent class in the class-path under the specified packages
     */
    @Override
    public void afterPropertiesSet() throws Exception {
        if (packages == null || packages.isEmpty())
            throw new IllegalArgumentException("packages property must be set");
        log.debug("Looking for @Entity in " + packages);
        persistentClasses = new HashSet<Class<? extends Object>>();
        ClassPathScanningCandidateComponentProvider scanner =
                new ClassPathScanningCandidateComponentProvider(false);

        scanner.addIncludeFilter(new AnnotationTypeFilter(Entity.class));

        for (String p : packages) {
            for (BeanDefinition bd : scanner.findCandidateComponents(p)) {
                persistentClasses.add(Class.forName(bd.getBeanClassName()));
            }
        }
        if (persistentClasses.isEmpty())
            throw new IllegalArgumentException("No class annotated with @Entity found in: " + packages);
    }

    /**
     * Add all the persistent classes found to the PersistentUnit
     */
    @Override
    public void postProcessPersistenceUnitInfo(MutablePersistenceUnitInfo persistenceUnitInfo) {
        for (Class<? extends Object> c : persistentClasses)
            persistenceUnitInfo.addManagedClassName(c.getName());
    }

    public void setPackages(List<String> packages) {
        this.packages = packages;
    }
}

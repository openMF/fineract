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
package org.apache.fineract.infrastructure.core.persistence;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.core.annotation.MergedAnnotation;
import org.springframework.core.annotation.MergedAnnotations;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.core.type.MethodMetadata;
import org.springframework.core.type.classreading.CachingMetadataReaderFactory;
import org.springframework.core.type.classreading.MetadataReaderFactory;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Build-time guard for the invariant that keeps {@code ExtendedJpaTransactionManager}'s lock-free EclipseLink
 * connection handle safe: <b>no transaction routed to the JPA transaction manager may request a non-default isolation
 * level.</b>
 *
 * <p>
 * A custom isolation on the JPA manager makes EclipseLink transiently mutate its shared per-session
 * {@code DatabaseLogin} isolation, which - without the connection-acquisition lock we removed - can bleed the wrong
 * isolation into a concurrent transaction. Transactions that genuinely need a specific isolation level must run through
 * the JDBC transaction manager ({@code jdbcTransactionManager}), which applies isolation per-connection.
 *
 * <p>
 * This scans all {@code org.apache.fineract} classes on the classpath (via ASM metadata, without loading them) and
 * fails if any {@code @Transactional} declares a non-default {@link Isolation} without explicitly targeting
 * {@code jdbcTransactionManager}. It complements the runtime guard in {@code ExtendedJpaTransactionManager#doBegin},
 * which additionally catches programmatic isolation (e.g. {@code TransactionTemplate#setIsolationLevel}) that
 * annotation scanning cannot see.
 */
public class NoCustomIsolationOnJpaTransactionManagerTest {

    private static final String JDBC_TRANSACTION_MANAGER = "jdbcTransactionManager";
    private static final String TRANSACTIONAL = Transactional.class.getName();

    @Test
    public void noCustomIsolationLevelIsUsedWithTheJpaTransactionManager() throws IOException {
        PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
        MetadataReaderFactory metadataReaderFactory = new CachingMetadataReaderFactory(resolver);
        Resource[] resources = resolver.getResources("classpath*:org/apache/fineract/**/*.class");

        List<String> violations = new ArrayList<>();
        for (Resource resource : resources) {
            if (!resource.isReadable()) {
                continue;
            }
            AnnotationMetadata metadata;
            try {
                metadata = metadataReaderFactory.getMetadataReader(resource).getAnnotationMetadata();
            } catch (Throwable t) { // NOSONAR - skip anything that cannot be parsed (module-info, synthetic, etc.)
                continue;
            }

            collectViolation(metadata.getClassName(), "<class-level @Transactional>", metadata.getAnnotations(), violations);
            for (MethodMetadata method : metadata.getAnnotatedMethods(TRANSACTIONAL)) {
                collectViolation(metadata.getClassName(), method.getMethodName() + "()", method.getAnnotations(), violations);
            }
        }

        assertThat(violations)
                .as("These @Transactional declarations use a non-default isolation level on the JPA transaction manager, which "
                        + "is unsafe with the lock-free EclipseLink connection handle. Route them through the JDBC transaction "
                        + "manager instead: @Transactional(transactionManager = \"jdbcTransactionManager\", isolation = ...). "
                        + "Offenders:\n%s", String.join("\n", violations))
                .isEmpty();
    }

    private static void collectViolation(String className, String location, MergedAnnotations annotations, List<String> violations) {
        MergedAnnotation<Transactional> transactional = annotations.get(Transactional.class);
        if (!transactional.isPresent()) {
            return;
        }
        Isolation isolation = transactional.getEnum("isolation", Isolation.class);
        if (isolation == Isolation.DEFAULT) {
            return;
        }
        String transactionManager = transactional.getString("transactionManager");
        if (!JDBC_TRANSACTION_MANAGER.equals(transactionManager)) {
            String target = transactionManager.isEmpty() ? "<default/primary JPA manager>" : transactionManager;
            violations.add("  " + className + "." + location + " -> isolation=" + isolation + ", transactionManager=" + target);
        }
    }
}

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
package org.apache.fineract.test.initializer;

import java.util.Arrays;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.fineract.test.initializer.base.FineractInitializer;
import org.apache.fineract.test.support.loader.FineractConfigLoader;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

@Slf4j
public final class FineractInitializerFactory {

    private FineractInitializerFactory() {}

    private static final class Holder {

        private Holder() {}

        static final FineractInitializer INSTANCE;

        static {
            log.info("=== FineractInitializerFactory: Loading configuration classes ===");
            Set<Class<?>> initializerConfigurationClasses = FineractConfigLoader.getInitializerConfigurationClasses();
            log.info("Configuration classes to load: {}", initializerConfigurationClasses);

            log.info("=== FineractInitializerFactory: Creating ApplicationContext ===");
            AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(
                    initializerConfigurationClasses.toArray(new Class<?>[0]));

            log.info("=== FineractInitializerFactory: ApplicationContext created ===");
            String[] beanNames = context.getBeanDefinitionNames();
            log.info("Total beans in context: {}", beanNames.length);

            // Log suite initializer beans specifically
            String[] suiteInitBeans = context
                    .getBeanNamesForType(org.apache.fineract.test.initializer.suite.FineractSuiteInitializerStep.class);
            log.info("Suite initializer beans found: {} - {}", suiteInitBeans.length, Arrays.toString(suiteInitBeans));

            // Log if FineractFeignClient bean exists
            String[] feignClientBeans = context.getBeanNamesForType(org.apache.fineract.client.feign.FineractFeignClient.class);
            log.info("FineractFeignClient beans found: {} - {}", feignClientBeans.length, Arrays.toString(feignClientBeans));

            log.info("=== FineractInitializerFactory: Getting FineractInitializer bean ===");
            INSTANCE = context.getBean(FineractInitializer.class);
            log.info("=== FineractInitializerFactory: FineractInitializer bean retrieved successfully ===");
        }
    }

    public static FineractInitializer get() {
        return Holder.INSTANCE;
    }
}

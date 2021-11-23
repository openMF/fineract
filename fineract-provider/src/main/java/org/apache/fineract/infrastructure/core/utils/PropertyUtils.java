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
package org.apache.fineract.infrastructure.core.utils;

import java.util.Properties;

import org.springframework.beans.factory.config.YamlPropertiesFactoryBean;
import org.springframework.core.env.Environment;
import org.springframework.core.io.ClassPathResource;

public class PropertyUtils {
    public static Properties loadYamlProperties(final String resourcePath) {
        ClassPathResource resource = new ClassPathResource(resourcePath);
        if (resource.exists()) {
            YamlPropertiesFactoryBean yaml = new YamlPropertiesFactoryBean();
            yaml.setResources(resource);
            return yaml.getObject();
        } else {
            return new Properties();
        }
    }

    public static String getString(final Environment env, final String key, final String defaultVal) {
        return env.getProperty(key, defaultVal);
    }

    public static Integer getInteger(final Environment env, final String key, final String defaultVal) {
        return Integer.valueOf(env.getProperty(key, defaultVal));
    }
}

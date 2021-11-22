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

import java.util.Arrays;

import org.apache.fineract.infrastructure.core.boot.JDBCDriverConfig;
import org.springframework.core.env.Environment;

public class ProfileUtils {

    private final Environment environment;

    public ProfileUtils(final Environment environment) {
        this.environment = environment;
    }

    public String[] getActiveProfiles() {
        return environment.getActiveProfiles();
    }

    public boolean isActiveProfile(String profile) {
        String[] activeProfiles = getActiveProfiles();
        return (Arrays.asList(activeProfiles).contains(profile));
    }

    public StringBuilder calcContext(JDBCDriverConfig driverConfig, StringBuilder context) {
        if (context == null) {
            context = new StringBuilder();
        } else if (context.length() > 0) {
            context.append(',');
        }

        context.append(driverConfig.getDialect().name().toLowerCase());

        String[] profiles = getActiveProfiles();
        if (profiles.length > 0) {
            context.append(',').append(String.join(",", profiles));
        }
        return context;
    }
}

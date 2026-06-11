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
package org.apache.fineract.cob.converter;

import org.apache.fineract.cob.data.COBParameter;

public final class COBParameterConverter {

    private static final String LEGACY_LOAN_COB_PARAMETER_CLASS_NAME = "org.apache.fineract.cob.data.LoanCOBParameter";
    private static final Class<?> LEGACY_LOAN_COB_PARAMETER_CLASS = findLegacyLoanCOBParameterClass();

    private COBParameterConverter() {}

    public static COBParameter convert(Object obj) {
        if (obj instanceof COBParameter) {
            return (COBParameter) obj;
        } else if (LEGACY_LOAN_COB_PARAMETER_CLASS != null && LEGACY_LOAN_COB_PARAMETER_CLASS.isInstance(obj)) {
            // for backward compatibility
            return convertLegacyLoanCOBParameter(obj);
        }
        return null;
    }

    private static Class<?> findLegacyLoanCOBParameterClass() {
        try {
            return Class.forName(LEGACY_LOAN_COB_PARAMETER_CLASS_NAME);
        } catch (ClassNotFoundException e) {
            return null;
        }
    }

    private static COBParameter convertLegacyLoanCOBParameter(Object obj) {
        try {
            return (COBParameter) LEGACY_LOAN_COB_PARAMETER_CLASS.getMethod("toCOBParameter").invoke(obj);
        } catch (ReflectiveOperationException e) {
            throw new IllegalArgumentException("Unable to convert legacy LoanCOBParameter", e);
        }
    }
}

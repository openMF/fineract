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
package org.apache.fineract.test.helper;

import java.math.BigDecimal;
import java.math.RoundingMode;

public final class BigDecimalHelper {

    private static final RoundingMode ROUNDING_MODE = RoundingMode.HALF_EVEN;

    private BigDecimalHelper() {}

    public static BigDecimal convert(Double value) {
        return BigDecimal.valueOf(value);
    }

    public static BigDecimal convert(Double value, int scale) {
        return BigDecimal.valueOf(value).setScale(scale, ROUNDING_MODE);
    }

    public static BigDecimal scale(BigDecimal input, int scale) {
        return input.setScale(scale, ROUNDING_MODE);
    }
}

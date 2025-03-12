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
import java.math.MathContext;
import java.math.RoundingMode;

public final class BigDecimalHelper {

    public static final RoundingMode ROUNDING_MODE = RoundingMode.HALF_EVEN;
    public static final int SCALE = 2;
    public static final MathContext MATH_CONTEXT = new MathContext(SCALE, ROUNDING_MODE);

    private BigDecimalHelper() {}

    public static BigDecimal scale(BigDecimal input, int scale) {
        return input.setScale(scale, ROUNDING_MODE);
    }

    public static BigDecimal valueOf(Double value, int scale) {
        return BigDecimal.valueOf(value).setScale(scale, ROUNDING_MODE);
    }

    public static BigDecimal valueOf(int value) {
        return BigDecimal.valueOf(value).setScale(SCALE, ROUNDING_MODE);
    }

    public static BigDecimal valueOf(long l) {
        return BigDecimal.valueOf(l).setScale(SCALE, ROUNDING_MODE);
    }

    public static BigDecimal valueOf(Double value) {
        return BigDecimal.valueOf(value).setScale(SCALE, ROUNDING_MODE);
    }

    public static BigDecimal valueOf(String value) {
        return new BigDecimal(value).setScale(SCALE, ROUNDING_MODE);
    }
}

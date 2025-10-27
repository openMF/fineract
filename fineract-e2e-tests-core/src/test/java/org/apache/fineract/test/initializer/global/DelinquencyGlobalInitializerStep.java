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
package org.apache.fineract.test.initializer.global;

import static org.apache.fineract.client.feign.util.FeignCalls.executeVoid;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.apache.fineract.client.feign.FineractFeignClient;
import org.apache.fineract.client.models.DelinquencyBucketRequest;
import org.apache.fineract.client.models.DelinquencyRangeRequest;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

@RequiredArgsConstructor
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class DelinquencyGlobalInitializerStep implements FineractGlobalInitializerStep {

    public static final String DEFAULT_LOCALE = "en";
    public static final List<Integer> DEFAULT_DELINQUENCY_RANGES = Arrays.asList(1, 3, 30, 60, 90, 120, 150, 180, 240);
    public static final String DEFAULT_DELINQUENCY_BUCKET_NAME = "Default delinquency bucket";

    private final FineractFeignClient fineractClient;

    @Override
    public void initialize() {
        setDefaultDelinquencyRanges();
        setDefaultDelinquencyBucket();
    }

    public void setDefaultDelinquencyRanges() {
        for (int i = 0; i < DEFAULT_DELINQUENCY_RANGES.size() - 1; i++) {
            DelinquencyRangeRequest postDelinquencyRangeRequest = new DelinquencyRangeRequest();
            postDelinquencyRangeRequest.classification("Delinquency range " + DEFAULT_DELINQUENCY_RANGES.get(i).toString());
            postDelinquencyRangeRequest.locale(DEFAULT_LOCALE);
            if (DEFAULT_DELINQUENCY_RANGES.get(i) == 1) {
                postDelinquencyRangeRequest.minimumAgeDays(1);
                postDelinquencyRangeRequest.maximumAgeDays(3);
            } else {
                postDelinquencyRangeRequest.minimumAgeDays(DEFAULT_DELINQUENCY_RANGES.get(i) + 1);
                postDelinquencyRangeRequest.maximumAgeDays(DEFAULT_DELINQUENCY_RANGES.get(i + 1));
            }

            executeVoid(() -> fineractClient.delinquencyRangeAndBucketsManagement().createDelinquencyRange(postDelinquencyRangeRequest));
        }

        DelinquencyRangeRequest lastRange = new DelinquencyRangeRequest();
        lastRange.classification("Delinquency range " + DEFAULT_DELINQUENCY_RANGES.get(DEFAULT_DELINQUENCY_RANGES.size() - 1).toString());
        lastRange.locale(DEFAULT_LOCALE);
        lastRange.minimumAgeDays(DEFAULT_DELINQUENCY_RANGES.get(DEFAULT_DELINQUENCY_RANGES.size() - 1) + 1);
        lastRange.maximumAgeDays(null);

        executeVoid(() -> fineractClient.delinquencyRangeAndBucketsManagement().createDelinquencyRange(lastRange));
    }

    public void setDefaultDelinquencyBucket() {
        List<Long> rangesNr = new ArrayList<>();

        for (int i = 1; i < DEFAULT_DELINQUENCY_RANGES.size() + 1; i++) {
            rangesNr.add((long) DEFAULT_DELINQUENCY_RANGES.indexOf(DEFAULT_DELINQUENCY_RANGES.get(i - 1)));
        }
        rangesNr.add((long) DEFAULT_DELINQUENCY_RANGES.size());

        DelinquencyBucketRequest postDelinquencyBucketRequest = new DelinquencyBucketRequest();
        postDelinquencyBucketRequest.name(DEFAULT_DELINQUENCY_BUCKET_NAME);
        postDelinquencyBucketRequest.ranges(rangesNr);

        executeVoid(() -> fineractClient.delinquencyRangeAndBucketsManagement().createDelinquencyBucket(postDelinquencyBucketRequest));
    }
}

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
package org.apache.fineract.integrationtests.client.feign.tests;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.List;
import org.apache.fineract.client.models.DelinquencyBucketRequest;
import org.apache.fineract.client.models.DelinquencyBucketResponse;
import org.apache.fineract.client.models.DelinquencyRangeRequest;
import org.apache.fineract.client.models.DelinquencyRangeResponse;
import org.apache.fineract.client.models.PostDelinquencyBucketResponse;
import org.apache.fineract.client.models.PostDelinquencyRangeResponse;
import org.apache.fineract.integrationtests.client.FeignIntegrationTest;
import org.apache.fineract.integrationtests.client.feign.helpers.FeignDelinquencyHelper;
import org.apache.fineract.integrationtests.common.Utils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class FeignDelinquencyLifecycleTest extends FeignIntegrationTest {

    private static final String LOCALE = "en";

    private FeignDelinquencyHelper delinquencyHelper;

    private Long rangeId1;
    private Long rangeId2;
    private Long bucketId;

    @BeforeAll
    void setup() {
        delinquencyHelper = new FeignDelinquencyHelper(fineractClient());
    }

    @Test
    @Order(1)
    void testCreateAndGetDelinquencyRange() {
        String classification = Utils.uniqueRandomStringGenerator("DLQ_R_", 8);

        PostDelinquencyRangeResponse created = delinquencyHelper.createRange(new DelinquencyRangeRequest()//
                .classification(classification)//
                .minimumAgeDays(1)//
                .maximumAgeDays(30)//
                .locale(LOCALE));

        assertNotNull(created);
        assertNotNull(created.getResourceId());
        rangeId1 = created.getResourceId();

        DelinquencyRangeResponse fetched = delinquencyHelper.getRange(rangeId1);
        assertNotNull(fetched);
        assertEquals(classification, fetched.getClassification());
        assertEquals(1, fetched.getMinimumAgeDays());
        assertEquals(30, fetched.getMaximumAgeDays());
    }

    @Test
    @Order(2)
    void testUpdateDelinquencyRange() {
        assertNotNull(rangeId1, "rangeId1 must be set by testCreateAndGetDelinquencyRange (Order 1)");

        String updatedClassification = Utils.uniqueRandomStringGenerator("DLQ_R_UPD_", 6);

        delinquencyHelper.updateRange(rangeId1, new DelinquencyRangeRequest()//
                .classification(updatedClassification)//
                .minimumAgeDays(1)//
                .maximumAgeDays(45)//
                .locale(LOCALE));

        DelinquencyRangeResponse updated = delinquencyHelper.getRange(rangeId1);
        assertNotNull(updated);
        assertEquals(updatedClassification, updated.getClassification());
        assertEquals(45, updated.getMaximumAgeDays());
    }

    @Test
    @Order(3)
    void testCreateDelinquencyBucketWithRanges() {
        rangeId1 = delinquencyHelper.createRange(new DelinquencyRangeRequest()//
                .classification(Utils.uniqueRandomStringGenerator("DLQ_R1_", 6))//
                .minimumAgeDays(1)//
                .maximumAgeDays(30)//
                .locale(LOCALE)).getResourceId();

        rangeId2 = delinquencyHelper.createRange(new DelinquencyRangeRequest()//
                .classification(Utils.uniqueRandomStringGenerator("DLQ_R2_", 6))//
                .minimumAgeDays(31)//
                .maximumAgeDays(60)//
                .locale(LOCALE)).getResourceId();

        assertNotNull(rangeId1);
        assertNotNull(rangeId2);

        String bucketName = Utils.uniqueRandomStringGenerator("DLQ_B_", 8);
        PostDelinquencyBucketResponse created = delinquencyHelper.createBucket(new DelinquencyBucketRequest()//
                .name(bucketName)//
                .ranges(List.of(rangeId1, rangeId2)));

        assertNotNull(created);
        assertNotNull(created.getResourceId());
        bucketId = created.getResourceId();

        DelinquencyBucketResponse fetched = delinquencyHelper.getBucket(bucketId);
        assertNotNull(fetched);
        assertEquals(bucketName, fetched.getName());
        assertNotNull(fetched.getRanges());
        assertEquals(2, fetched.getRanges().size());
    }

    @Test
    @Order(4)
    void testUpdateDelinquencyBucket() {
        assertNotNull(bucketId, "bucketId must be set by testCreateDelinquencyBucketWithRanges (Order 3)");
        assertNotNull(rangeId1, "rangeId1 must be set by testCreateDelinquencyBucketWithRanges (Order 3)");

        String updatedName = Utils.uniqueRandomStringGenerator("DLQ_B_UPD_", 6);

        delinquencyHelper.updateBucket(bucketId, new DelinquencyBucketRequest()//
                .name(updatedName)//
                .ranges(List.of(rangeId1)));

        DelinquencyBucketResponse updated = delinquencyHelper.getBucket(bucketId);
        assertNotNull(updated);
        assertEquals(updatedName, updated.getName());
        assertNotNull(updated.getRanges());
        assertEquals(1, updated.getRanges().size());
    }

    @Test
    @Order(5)
    void testDeleteDelinquencyBucket() {
        assertNotNull(bucketId, "bucketId must be set by testCreateDelinquencyBucketWithRanges (Order 3)");

        delinquencyHelper.deleteBucket(bucketId);

        List<DelinquencyBucketResponse> buckets = delinquencyHelper.getBuckets();
        assertFalse(buckets.stream().anyMatch(b -> bucketId.equals(b.getId())),
                "Deleted bucket " + bucketId + " should no longer appear in the list");
    }

    @Test
    @Order(6)
    void testDeleteDelinquencyRanges() {
        assertNotNull(rangeId1, "rangeId1 must be set by testCreateDelinquencyBucketWithRanges (Order 3)");
        assertNotNull(rangeId2, "rangeId2 must be set by testCreateDelinquencyBucketWithRanges (Order 3)");

        delinquencyHelper.deleteRange(rangeId1);
        delinquencyHelper.deleteRange(rangeId2);

        List<DelinquencyRangeResponse> ranges = delinquencyHelper.getRanges();
        assertFalse(ranges.stream().anyMatch(r -> rangeId1.equals(r.getId())),
                "Deleted range " + rangeId1 + " should no longer appear in the list");
        assertFalse(ranges.stream().anyMatch(r -> rangeId2.equals(r.getId())),
                "Deleted range " + rangeId2 + " should no longer appear in the list");
    }
}

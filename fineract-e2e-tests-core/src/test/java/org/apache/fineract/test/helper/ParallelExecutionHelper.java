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

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Function;

public final class ParallelExecutionHelper {

    private ParallelExecutionHelper() {}

    private static final int CLEANUP_THREAD_COUNT = 10;

    public static <T, R> void runInParallel(List<T> items, Consumer<T> action) {
        mapInParallel(items, id -> {
            action.accept(id);
            return null;
        });
    }

    public static void runInParallel(List<Runnable> items) {
        runInParallel(items, Runnable::run);
    }

    private static <T, R> List<R> mapInParallel(List<T> items, Function<T, R> action) {
        if (items.isEmpty()) {
            return List.of();
        }
        try (ExecutorService executor = Executors.newFixedThreadPool(Math.min(CLEANUP_THREAD_COUNT, items.size()))) {
            List<Future<R>> futures = items.stream().map(item -> executor.submit(() -> action.apply(item))).toList();
            return futures.stream().map(ParallelExecutionHelper::getFutureResult).toList();
        }
    }

    private static <T> T getFutureResult(Future<T> future) {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted during parallel loan cleanup", e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("Parallel loan cleanup failed", e);
        }
    }
}

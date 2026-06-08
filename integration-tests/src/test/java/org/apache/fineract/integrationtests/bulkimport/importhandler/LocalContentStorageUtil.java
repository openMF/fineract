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
package org.apache.fineract.integrationtests.bulkimport.importhandler;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class LocalContentStorageUtil {

    private static final long DEFAULT_MAX_WAIT_MILLIS = 10000;
    private static final long POLL_INTERVAL_MILLIS = 500;

    private LocalContentStorageUtil() {}

    public static String path(String path) {
        var currentPath = Path.of("").toAbsolutePath();
        Path resolvedPath = find(path, currentPath);
        if (resolvedPath != null) {
            return resolvedPath.toString();
        }

        throw notFound(path, currentPath);
    }

    public static String waitForPath(String path) throws InterruptedException {
        var currentPath = Path.of("").toAbsolutePath();
        long start = System.currentTimeMillis();
        Path previousPath = null;
        long previousSize = -1;

        while ((System.currentTimeMillis() - start) < DEFAULT_MAX_WAIT_MILLIS) {
            Path resolvedPath = find(path, currentPath);
            long size = resolvedPath == null ? -1 : size(resolvedPath);
            if (size > 0 && resolvedPath.equals(previousPath) && size == previousSize) {
                return resolvedPath.toString();
            }

            previousPath = resolvedPath;
            previousSize = size;
            Thread.sleep(POLL_INTERVAL_MILLIS);
        }

        throw notFound(path, currentPath);
    }

    private static Path find(String path, Path currentPath) {
        for (Path candidate : candidates(path, currentPath)) {
            if (Files.exists(candidate)) {
                return candidate;
            }
        }
        return null;
    }

    private static List<Path> candidates(String path, Path currentPath) {
        List<Path> candidates = new ArrayList<>();
        candidates.add(Path.of(path));
        candidates.add(Path.of("/", path));
        candidates.add(Path.of("/home/runner/.fineract/DefaultDemoTenant").resolve(path));
        candidates.add(Path.of(System.getProperty("user.home")).toAbsolutePath().resolve(".fineract/DefaultDemoTenant").resolve(path));

        String dockerContentRoot = System.getenv("FINERACT_TEST_CONTENT_ROOT");
        if (dockerContentRoot != null && !dockerContentRoot.isBlank()) {
            candidates.add(Path.of(dockerContentRoot).resolve("DefaultDemoTenant").resolve(path));
        }

        candidates.add(currentPath.resolve("build/fineract/tmp/DefaultDemoTenant").resolve(path));
        Path parentPath = currentPath.getParent();
        if (parentPath != null) {
            candidates.add(parentPath.resolve("build/fineract/tmp/DefaultDemoTenant").resolve(path));
        }
        return candidates;
    }

    private static RuntimeException notFound(String path, Path currentPath) {
        return new RuntimeException("Cannot find local fineract path: " + path + " (" + currentPath + ")");
    }

    private static long size(Path path) {
        try {
            return Files.size(path);
        } catch (IOException e) {
            return -1;
        }
    }

    @SuppressWarnings("UnusedMethod")
    public static void waitFor(String path) throws InterruptedException {
        waitForPath(path);
        log.info("File found!");
    }
}

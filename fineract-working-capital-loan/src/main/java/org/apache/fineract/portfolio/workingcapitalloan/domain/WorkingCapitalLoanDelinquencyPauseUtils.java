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
package org.apache.fineract.portfolio.workingcapitalloan.domain;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import org.apache.fineract.portfolio.delinquency.domain.DelinquencyAction;

public final class WorkingCapitalLoanDelinquencyPauseUtils {

    private WorkingCapitalLoanDelinquencyPauseUtils() {}

    public static boolean isPauseActiveOnDate(final LocalDate pauseStart, final LocalDate pauseEnd, final LocalDate date) {
        return pauseStart != null && pauseEnd != null && date != null && !date.isBefore(pauseStart) && !date.isAfter(pauseEnd);
    }

    /**
     * Returns whether a new inclusive pause period shares at least one day with an existing one. Touching periods
     * (where one ends on the day the other starts) are treated as overlapping.
     */
    public static boolean inclusivePausePeriodsOverlap(final LocalDate parsedPauseStart, final LocalDate parsedPauseEnd,
            final LocalDate existingPauseStart, final LocalDate existingPauseEnd) {
        if (parsedPauseStart == null || parsedPauseEnd == null || existingPauseStart == null || existingPauseEnd == null) {
            return false;
        }
        return !parsedPauseStart.isAfter(existingPauseEnd) && !parsedPauseEnd.isBefore(existingPauseStart);
    }

    public static LocalDate resolveEffectivePauseEnd(final WorkingCapitalLoanDelinquencyAction pause,
            final List<WorkingCapitalLoanDelinquencyAction> actions) {
        final LocalDate pauseStart = pause.getStartDate();
        final LocalDate pauseEnd = pause.getEndDate();
        if (pauseStart == null || pauseEnd == null) {
            return pauseEnd;
        }
        return actions.stream().filter(Objects::nonNull)
                .filter(action -> DelinquencyAction.RESUME.equals(action.getAction()) && action.getStartDate() != null
                        && !action.getStartDate().isBefore(pauseStart) && !action.getStartDate().isAfter(pauseEnd))
                .map(WorkingCapitalLoanDelinquencyAction::getStartDate).min(Comparator.naturalOrder()).orElse(pauseEnd);
    }

    /**
     * Inclusive pause length: both start and end dates count as paused days.
     */
    public static long calculatePauseExtensionDays(final LocalDate pauseStart, final LocalDate pauseEnd) {
        if (pauseStart == null || pauseEnd == null || pauseStart.isAfter(pauseEnd)) {
            return 0L;
        }
        return ChronoUnit.DAYS.between(pauseStart, pauseEnd) + 1;
    }

    /**
     * Days to remove from the schedule when a pause is resumed early. Uses the difference between the full planned
     * inclusive pause and the effective inclusive pause ending on the resume date.
     */
    public static long calculateDaysRemovedOnResume(final LocalDate pauseStart, final LocalDate resumeDate,
            final LocalDate originalPauseEnd) {
        if (pauseStart == null || resumeDate == null || originalPauseEnd == null || resumeDate.isAfter(originalPauseEnd)) {
            return 0L;
        }
        return calculatePauseExtensionDays(pauseStart, originalPauseEnd) - calculatePauseExtensionDays(pauseStart, resumeDate);
    }

}

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
package org.apache.fineract.infrastructure.core.service;

import static java.time.temporal.ChronoUnit.DAYS;

import jakarta.validation.constraints.NotNull;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import org.apache.fineract.infrastructure.core.data.ApiParameterError;
import org.apache.fineract.infrastructure.core.domain.FineractPlatformTenant;
import org.apache.fineract.infrastructure.core.exception.PlatformApiDataValidationException;

public final class DateUtils {

    public static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd";
    public static final String DATETIME_FORMAT_SECONDS = DEFAULT_DATE_FORMAT + " HH:mm:ss";
    static final String DEFAULT_DATETIME_FORMAT = DEFAULT_DATE_FORMAT + " HH[:mm][:ss][.SSS]";
    static final String DEFAULT_OFFSET_DATETIME_FORMAT = DEFAULT_DATETIME_FORMAT + "XXX";
    static final DateTimeFormatter DEFAULT_DATE_FORMATTER = DateTimeFormatter.ofPattern(DEFAULT_DATE_FORMAT);
    static final DateTimeFormatter DEFAULT_DATETIME_FORMATTER = DateTimeFormatter.ofPattern(DEFAULT_DATETIME_FORMAT);
    static final DateTimeFormatter DEFAULT_OFFSET_DATETIME_FORMATTER = DateTimeFormatter.ofPattern(DEFAULT_OFFSET_DATETIME_FORMAT);

    private DateUtils() {

    }

    public static ZoneId getSystemZoneId() {
        return ZoneId.systemDefault();
    }

    public static ZoneId getDateTimeZoneOfTenant() {
        final FineractPlatformTenant tenant = ThreadLocalContextUtil.getTenant();
        return ZoneId.of(tenant.getTimezoneId());
    }

    public static ZoneOffset getAuditZoneId() {
        return ZoneOffset.UTC;
    }

    // LocalDateTime

    public static LocalDateTime getLocalDateTimeOfTenant() {
        return getLocalDateTimeOfTenant(null);
    }

    public static LocalDateTime getLocalDateTimeOfTenant(ChronoUnit truncate) {
        LocalDateTime now = LocalDateTime.now(getDateTimeZoneOfTenant());
        return truncate == null ? now : now.truncatedTo(truncate);
    }

    public static LocalDateTime getLocalDateTimeOfSystem() {
        return getLocalDateTimeOfSystem(null);
    }

    public static LocalDateTime getLocalDateTimeOfSystem(ChronoUnit truncate) {
        LocalDateTime now = LocalDateTime.now(ZoneId.systemDefault());
        return truncate == null ? now : now.truncatedTo(truncate);
    }

    public static LocalDateTime getAuditLocalDateTime() {
        return LocalDateTime.now(getAuditZoneId());
    }

    public static int compare(LocalDateTime first, LocalDateTime second) {
        return compare(first, second, null);
    }

    public static int compare(LocalDateTime first, LocalDateTime second, ChronoUnit truncate) {
        if (first == null) {
            return second == null ? 0 : -1;
        }
        if (second == null) {
            return 1;
        }
        return truncate == null ? first.compareTo(second) : first.truncatedTo(truncate).compareTo(second.truncatedTo(truncate));
    }

    public static boolean isEqual(LocalDateTime first, LocalDateTime second) {
        return isEqual(first, second, null);
    }

    public static boolean isEqual(LocalDateTime first, LocalDateTime second, ChronoUnit truncate) {
        return compare(first, second, truncate) == 0;
    }

    public static boolean isEqualTenantDateTime(LocalDateTime dateTime) {
        return isEqualTenantDateTime(dateTime, null);
    }

    public static boolean isEqualTenantDateTime(LocalDateTime dateTime, ChronoUnit truncate) {
        return isEqual(dateTime, getLocalDateTimeOfTenant(), truncate);
    }

    public static boolean isEqualSystemDateTime(LocalDateTime dateTime) {
        return isEqualSystemDateTime(dateTime, null);
    }

    public static boolean isEqualSystemDateTime(LocalDateTime dateTime, ChronoUnit truncate) {
        return isEqual(dateTime, getLocalDateTimeOfSystem(), truncate);
    }

    public static boolean isBefore(LocalDateTime first, LocalDateTime second) {
        return isBefore(first, second, null);
    }

    public static boolean isBefore(LocalDateTime first, LocalDateTime second, ChronoUnit truncate) {
        return compare(first, second, truncate) < 0;
    }

    public static boolean isBeforeTenantDateTime(LocalDateTime dateTime) {
        return isBeforeTenantDateTime(dateTime, null);
    }

    public static boolean isBeforeTenantDateTime(LocalDateTime dateTime, ChronoUnit truncate) {
        return isBefore(dateTime, getLocalDateTimeOfTenant(), truncate);
    }

    public static boolean isBeforeSystemDateTime(LocalDateTime dateTime) {
        return isBeforeSystemDateTime(dateTime, null);
    }

    public static boolean isBeforeSystemDateTime(LocalDateTime dateTime, ChronoUnit truncate) {
        return isBefore(dateTime, getLocalDateTimeOfSystem(), truncate);
    }

    public static boolean isAfter(LocalDateTime first, LocalDateTime second) {
        return isAfter(first, second, null);
    }

    public static boolean isAfter(LocalDateTime first, LocalDateTime second, ChronoUnit truncate) {
        return compare(first, second, truncate) > 0;
    }

    public static boolean isAfterTenantDateTime(LocalDateTime dateTime) {
        return isAfterTenantDateTime(dateTime, null);
    }

    public static boolean isAfterTenantDateTime(LocalDateTime dateTime, ChronoUnit truncate) {
        return isAfter(dateTime, getLocalDateTimeOfTenant(), truncate);
    }

    public static boolean isAfterSystemDateTime(LocalDateTime dateTime) {
        return isAfterSystemDateTime(dateTime, null);
    }

    public static boolean isAfterSystemDateTime(LocalDateTime dateTime, ChronoUnit truncate) {
        return isAfter(dateTime, getLocalDateTimeOfSystem(), truncate);
    }

    public static LocalDateTime toLocalDateTime(Timestamp timestamp) {
        return timestamp == null ? null : timestamp.toLocalDateTime();
    }

    public static LocalDateTime toLocalDateTime(OffsetDateTime dateTime) {
        return dateTime == null ? null : toSystemOffsetDateTime(dateTime).toLocalDateTime();
    }

    public static LocalTime toLocalTime(Timestamp timestamp) {
        return timestamp == null ? null : toLocalDateTime(timestamp).toLocalTime();
    }

    public static ZonedDateTime toTenantZonedDateTime(LocalDateTime dateTime) {
        return dateTime == null ? null : ZonedDateTime.of(dateTime, getDateTimeZoneOfTenant());
    }

    // OffsetDateTime

    public static OffsetDateTime getTenantOffsetDateTime() {
        return getTenantOffsetDateTime(null);
    }

    public static OffsetDateTime getTenantOffsetDateTime(ChronoUnit truncate) {
        OffsetDateTime now = OffsetDateTime.now(getDateTimeZoneOfTenant());
        return truncate == null ? now : now.truncatedTo(truncate);
    }

    public static OffsetDateTime getAuditOffsetDateTime() {
        return OffsetDateTime.now(getAuditZoneId());
    }

    public static int compare(OffsetDateTime first, OffsetDateTime second) {
        return compare(first, second, null);
    }

    public static int compare(OffsetDateTime first, OffsetDateTime second, ChronoUnit truncate) {
        return compare(first, second, truncate, true);
    }

    public static int compareNullLast(OffsetDateTime first, OffsetDateTime second) {
        return compare(first, second, null, false);
    }

    public static int compareNullLast(@NotNull Optional<OffsetDateTime> first, @NotNull Optional<OffsetDateTime> second) {
        return compareNullLast(first.orElse(null), second.orElse(null));
    }

    public static int compare(OffsetDateTime first, OffsetDateTime second, ChronoUnit truncate, boolean nullFirst) {
        if (first == null) {
            return second == null ? 0 : (nullFirst ? -1 : 1);
        }
        if (second == null) {
            return nullFirst ? 1 : -1;
        }
        first = first.withOffsetSameInstant(ZoneOffset.UTC);
        second = second.withOffsetSameInstant(ZoneOffset.UTC);
        return truncate == null ? first.compareTo(second) : first.truncatedTo(truncate).compareTo(second.truncatedTo(truncate));
    }

    public static boolean isEqual(OffsetDateTime first, OffsetDateTime second) {
        return isEqual(first, second, null);
    }

    public static boolean isEqual(OffsetDateTime first, OffsetDateTime second, ChronoUnit truncate) {
        return compare(first, second, truncate) == 0;
    }

    public static boolean isEqualTenantDateTime(OffsetDateTime dateTime) {
        return isEqualTenantDateTime(dateTime, null);
    }

    public static boolean isEqualTenantDateTime(OffsetDateTime dateTime, ChronoUnit truncate) {
        return isEqual(dateTime, getTenantOffsetDateTime(), truncate);
    }

    public static boolean isBefore(OffsetDateTime first, OffsetDateTime second) {
        return isBefore(first, second, null);
    }

    public static boolean isBefore(OffsetDateTime first, OffsetDateTime second, ChronoUnit truncate) {
        return compare(first, second, truncate) < 0;
    }

    public static boolean isBeforeTenantDateTime(OffsetDateTime dateTime) {
        return isBeforeTenantDateTime(dateTime, null);
    }

    public static boolean isBeforeTenantDateTime(OffsetDateTime dateTime, ChronoUnit truncate) {
        return isBefore(dateTime, getTenantOffsetDateTime(), truncate);
    }

    public static boolean isAfter(OffsetDateTime first, OffsetDateTime second) {
        return isAfter(first, second, null);
    }

    public static boolean isAfter(OffsetDateTime first, OffsetDateTime second, ChronoUnit truncate) {
        return compare(first, second, truncate) > 0;
    }

    public static boolean isAfterTenantDateTime(OffsetDateTime dateTime) {
        return isAfterTenantDateTime(dateTime, null);
    }

    public static boolean isAfterTenantDateTime(OffsetDateTime dateTime, ChronoUnit truncate) {
        return isAfter(dateTime, getTenantOffsetDateTime(), truncate);
    }

    public static OffsetDateTime toTenantOffsetDateTime(Timestamp timestamp) {
        return toTenantOffsetDateTime(timestamp, getSystemZoneId());
    }

    public static OffsetDateTime toTenantOffsetDateTime(Timestamp timestamp, @NotNull ZoneId from) {
        return timestamp == null ? null : toTenantOffsetDateTime(timestamp.toLocalDateTime(), from);
    }

    public static OffsetDateTime toTenantOffsetDateTime(LocalDateTime localDateTime) {
        return toTenantOffsetDateTime(localDateTime, getSystemZoneId());
    }

    public static OffsetDateTime toTenantOffsetDateTime(LocalDateTime localDateTime, @NotNull ZoneId from) {
        if (localDateTime == null) {
            return null;
        }
        OffsetDateTime fromOffset = OffsetDateTime.of(localDateTime, from.getRules().getOffset(localDateTime));
        return toTenantOffsetDateTime(fromOffset);
    }

    @NotNull
    public static OffsetDateTime toTenantOffsetDateTime(@NotNull final LocalDate date) {
        return OffsetDateTime.of(date.atStartOfDay(), getTenantOffsetDateTime().getOffset());
    }

    public static OffsetDateTime toTenantOffsetDateTime(OffsetDateTime offsetDateTime) {
        return offsetDateTime == null ? null
                : offsetDateTime.withOffsetSameInstant(getDateTimeZoneOfTenant().getRules().getOffset(offsetDateTime.toInstant()));
    }

    public static OffsetDateTime toSystemOffsetDateTime(OffsetDateTime offsetDateTime) {
        return offsetDateTime == null ? null
                : offsetDateTime.withOffsetSameInstant(getSystemZoneId().getRules().getOffset(offsetDateTime.toInstant()));
    }

    public static OffsetDateTime plusDays(OffsetDateTime offsetDateTime, int days) {
        return offsetDateTime == null ? null : offsetDateTime.plusDays(days);
    }

    public static OffsetDateTime minusDays(OffsetDateTime offsetDateTime, int days) {
        return offsetDateTime == null ? null : offsetDateTime.minusDays(days);
    }

    // LocalDate

    public static LocalDate getLocalDateOfTenant() {
        return LocalDate.now(getDateTimeZoneOfTenant());
    }

    public static LocalDate getBusinessLocalDate() {
        return ThreadLocalContextUtil.getBusinessDate();
    }

    public static int compareToBusinessDate(LocalDate date) {
        return compare(date, getBusinessLocalDate());
    }

    public static boolean isEqualTenantDate(LocalDate date) {
        return isEqual(date, getLocalDateOfTenant());
    }

    public static boolean isBeforeTenantDate(LocalDate date) {
        return isBefore(date, getLocalDateOfTenant());
    }

    public static boolean isAfterTenantDate(LocalDate date) {
        return isAfter(date, getLocalDateOfTenant());
    }

    public static boolean isEqualBusinessDate(LocalDate date) {
        return isEqual(date, getBusinessLocalDate());
    }

    public static boolean isBeforeBusinessDate(LocalDate date) {
        return isBefore(date, getBusinessLocalDate());
    }

    public static boolean isAfterBusinessDate(LocalDate date) {
        return isAfter(date, getBusinessLocalDate());
    }

    public static boolean isDateInTheFuture(final LocalDate localDate) {
        return isAfterBusinessDate(localDate);
    }

    public static boolean isDateInThePast(final LocalDate localDate) {
        return isBeforeBusinessDate(localDate);
    }

    public static int compare(LocalDate first, LocalDate second) {
        return compare(first, second, true);
    }

    /**
     * Comparing dates. Null will be considered as last elements
     *
     * @param first
     * @param second
     * @return
     */
    public static int compareNullLast(LocalDate first, LocalDate second) {
        return compare(first, second, false);
    }

    public static int compare(LocalDate first, LocalDate second, boolean nullFirst) {
        return first == null ? (second == null ? 0 : (nullFirst ? -1 : 1))
                : (second == null ? (nullFirst ? 1 : -1) : first.compareTo(second));
    }

    public static boolean isEqual(LocalDate first, LocalDate second) {
        return first == null ? second == null : (second != null && first.isEqual(second));
    }

    public static boolean isBefore(LocalDate first, LocalDate second) {
        return second != null && (first == null || first.isBefore(second));
    }

    public static boolean isAfter(LocalDate first, LocalDate second) {
        return first != null && (second == null || first.isAfter(second));
    }

    public static boolean isAfterInclusive(LocalDate first, LocalDate second) {
        return isAfter(first, second) || isEqual(first, second);
    }

    public static long getDifference(LocalDate first, LocalDate second, @NotNull ChronoUnit unit) {
        if (first == null || second == null) {
            throw new IllegalArgumentException("Dates must not be null to get difference");
        }
        return unit.between(first, second);
    }

    public static int getExactDifference(LocalDate first, LocalDate second, @NotNull ChronoUnit unit) {
        return Math.toIntExact(getDifference(first, second, unit));
    }

    public static long getDifferenceInDays(LocalDate first, LocalDate second) {
        return getDifference(first, second, DAYS);
    }

    public static int getExactDifferenceInDays(LocalDate first, LocalDate second) {
        return getExactDifference(first, second, DAYS);
    }

    public static LocalDate plusDays(LocalDate localDate, int days) {
        return localDate == null ? null : localDate.plusDays(days);
    }

    public static LocalDate minusDays(LocalDate localDate, int days) {
        return localDate == null ? null : localDate.minusDays(days);
    }

    public static LocalDate toLocalDate(Date date) {
        return date == null ? null : date.toLocalDate();
    }

    public static ZonedDateTime toTenantZonedDateTime(Timestamp timestamp) {
        return toTenantZonedDateTime(toLocalDateTime(timestamp));
    }

    /**
     * Checks if a specific date falls within a given range (inclusive).
     *
     * @param targetDate
     *            the date to be checked
     * @param fromDate
     *            the start date of the range
     * @param toDate
     *            the end date of the range
     * @return true if targetDate is within range or equal to start/end dates, otherwise false
     */
    public static boolean isDateWithinRange(LocalDate targetDate, LocalDate fromDate, LocalDate toDate) {
        if (targetDate == null || fromDate == null || toDate == null) {
            throw new IllegalArgumentException("Dates must not be null");
        }
        return isDateInRangeInclusive(targetDate, fromDate, toDate);
    }

    public static boolean isDateInRangeInclusive(LocalDate targetDate, LocalDate fromDate, LocalDate toDate) {
        return fromDate != null && !DateUtils.isBefore(targetDate, fromDate) && !DateUtils.isAfter(targetDate, toDate);
    }

    public static boolean isDateInRangeExclusive(LocalDate targetDate, LocalDate fromDate, LocalDate toDate) {
        return fromDate != null && DateUtils.isAfter(targetDate, fromDate) && DateUtils.isBefore(targetDate, toDate);
    }

    public static boolean isDateInRangeFromExclusiveToInclusive(LocalDate targetDate, LocalDate fromDate, LocalDate toDate) {
        return fromDate != null && DateUtils.isAfter(targetDate, fromDate) && !DateUtils.isAfter(targetDate, toDate);
    }

    // Parse, format

    public static LocalDateTime parseLocalDateTime(String stringDateTime) {
        return parseLocalDateTime(stringDateTime, null);
    }

    public static LocalDateTime parseLocalDateTime(String stringDateTime, String format) {
        return parseLocalDateTime(stringDateTime, format, null);
    }

    public static LocalDateTime parseLocalDateTime(String stringDateTime, String format, Locale locale) {
        if (stringDateTime == null) {
            return null;
        }
        DateTimeFormatter formatter = getDateTimeFormatter(format, locale);
        try {
            return LocalDateTime.parse(stringDateTime, formatter);
        } catch (final DateTimeParseException e) {
            final List<ApiParameterError> errors = List.of(ApiParameterError.parameterError("validation.msg.invalid.datetime.pattern",
                    "The parameter dateTime (" + stringDateTime + ") format is invalid", "dateTime", stringDateTime));
            throw new PlatformApiDataValidationException(errors, e);
        }
    }

    /**
     * Parse LocalDateTime from any format (zoned or not-zoned) at system zone.
     */
    public static LocalDateTime parseInputLocalDateTime(String stringDateTime, String format, Locale locale) throws ParseException {
        return stringDateTime == null ? null
                : getSimpleDateFormat(format, locale).parse(stringDateTime).toInstant().atZone(getSystemZoneId()).toLocalDateTime();
    }

    public static LocalDate parseLocalDate(String stringDate) {
        return parseLocalDate(stringDate, null);
    }

    public static LocalDate parseLocalDate(String stringDate, String format) {
        return parseLocalDate(stringDate, format, null);
    }

    public static LocalDate parseLocalDate(String stringDate, String format, Locale locale) {
        if (stringDate == null) {
            return null;
        }
        DateTimeFormatter formatter = getDateFormatter(format, locale);
        try {
            return LocalDate.parse(stringDate, formatter);
        } catch (final DateTimeParseException e) {
            final List<ApiParameterError> errors = List.of(ApiParameterError.parameterError("validation.msg.invalid.date.pattern",
                    "The parameter date (" + stringDate + ") format is invalid", "date", stringDate));
            throw new PlatformApiDataValidationException("validation.msg.validation.errors.exist", "Validation errors exist.", errors, e);
        }
    }

    public static OffsetDateTime parseOffsetDateTime(String stringDate) {
        return parseOffsetDateTime(stringDate, null);
    }

    public static OffsetDateTime parseOffsetDateTime(String stringDate, String format) {
        return parseOffsetDateTime(stringDate, format, null);
    }

    public static OffsetDateTime parseOffsetDateTime(String stringDateTime, String format, Locale locale) {
        if (stringDateTime == null) {
            return null;
        }
        DateTimeFormatter formatter = getOffsetDateTimeFormatter(format, locale);
        try {
            return OffsetDateTime.parse(stringDateTime, formatter);
        } catch (final DateTimeParseException e) {
            final List<ApiParameterError> errors = List.of(ApiParameterError.parameterError("validation.msg.invalid.datetime.pattern",
                    "The parameter dateTime (" + stringDateTime + ") format is invalid", "dateTime", stringDateTime));
            throw new PlatformApiDataValidationException("validation.msg.validation.errors.exist", "Validation errors exist.", errors, e);
        }
    }

    public static String format(LocalDate date) {
        return format(date, null);
    }

    public static String format(LocalDate date, String format) {
        return format(date, format, null);
    }

    public static String format(LocalDate date, String format, Locale locale) {
        return date == null ? null : date.format(getDateFormatter(format, locale));
    }

    public static String format(LocalDateTime dateTime) {
        return format(dateTime, null);
    }

    public static String format(LocalDateTime dateTime, String format) {
        return format(dateTime, format, null);
    }

    public static String format(LocalDateTime dateTime, String format, Locale locale) {
        return dateTime == null ? null : dateTime.format(getDateTimeFormatter(format, locale));
    }

    public static String format(OffsetDateTime offsetDateTime) {
        return format(offsetDateTime, null);
    }

    public static String format(OffsetDateTime offsetDateTime, String format) {
        return format(offsetDateTime, format, null);
    }

    public static String format(OffsetDateTime offsetDateTime, String format, Locale locale) {
        return offsetDateTime == null ? null : offsetDateTime.format(getOffsetDateTimeFormatter(format, locale));
    }

    @NotNull
    private static DateTimeFormatter getDateFormatter(String format, Locale locale) {
        DateTimeFormatter formatter = DEFAULT_DATE_FORMATTER;
        if (format != null || locale != null) {
            if (format == null) {
                format = DEFAULT_DATE_FORMAT;
            }
            formatter = locale == null ? DateTimeFormatter.ofPattern(format) : DateTimeFormatter.ofPattern(format, locale);
        }
        return formatter;
    }

    @NotNull
    private static String getDateTimeFormat(String format) {
        return format == null ? DEFAULT_DATETIME_FORMAT : format;
    }

    @NotNull
    private static SimpleDateFormat getSimpleDateFormat(String format, Locale locale) {
        format = getDateTimeFormat(format);
        return locale == null ? new SimpleDateFormat(format) : new SimpleDateFormat(format, locale);
    }

    @NotNull
    private static DateTimeFormatter getDateTimeFormatter(String format, Locale locale) {
        DateTimeFormatter formatter = DEFAULT_DATETIME_FORMATTER;
        if (format != null || locale != null) {
            if (format == null) {
                format = DEFAULT_DATETIME_FORMAT;
            }
            formatter = locale == null ? DateTimeFormatter.ofPattern(format) : DateTimeFormatter.ofPattern(format, locale);
        }
        return formatter;
    }

    @NotNull
    private static DateTimeFormatter getOffsetDateTimeFormatter(String format, Locale locale) {
        DateTimeFormatter formatter = format == null ? DEFAULT_OFFSET_DATETIME_FORMATTER : DateTimeFormatter.ofPattern(format);
        if (locale != null) {
            formatter = formatter.withLocale(locale);
        }
        return formatter;
    }
}

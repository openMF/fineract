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

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.apache.fineract.infrastructure.core.data.ApiParameterError;
import org.apache.fineract.infrastructure.core.domain.FineractPlatformTenant;
import org.apache.fineract.infrastructure.core.exception.PlatformApiDataValidationException;

public final class DateUtils {

    private DateUtils() {

    }

    public static ZoneId getDateTimeZoneOfTenant() {
        final FineractPlatformTenant tenant = ThreadLocalContextUtil.getTenant();
        ZoneId zone = ZoneId.systemDefault();
        if (tenant != null) {
            zone = ZoneId.of(tenant.getTimezoneId());
        }
        return zone;
    }

    public static TimeZone getTimeZoneOfTenant() {
        final FineractPlatformTenant tenant = ThreadLocalContextUtil.getTenant();
        TimeZone zone = null;
        if (tenant != null) {
            zone = TimeZone.getTimeZone(tenant.getTimezoneId());
        }
        return zone;
    }

    public static Date getDateOfTenant() {
        return Date.from(getLocalDateOfTenant().atStartOfDay(getDateTimeZoneOfTenant()).toInstant());
    }

    public static LocalDate getLocalDateOfTenant() {
        final ZoneId zone = getDateTimeZoneOfTenant();
        LocalDate today = LocalDate.now(zone);
        return today;
    }

    public static LocalDateTime getLocalDateTimeOfTenant() {
        final ZoneId zone = getDateTimeZoneOfTenant();
        LocalDateTime today = LocalDateTime.now(zone);
        return today;
    }

    public static LocalDate parseLocalDate(final String stringDate, final String pattern) {

        try {
            final DateTimeFormatter dateStringFormat = DateTimeFormatter.ofPattern(pattern).withZone(getDateTimeZoneOfTenant());
            final ZonedDateTime dateTime = ZonedDateTime.parse(stringDate, dateStringFormat);
            return dateTime.toLocalDate();
        } catch (final IllegalArgumentException e) {
            final List<ApiParameterError> dataValidationErrors = new ArrayList<>();
            final ApiParameterError error = ApiParameterError.parameterError("validation.msg.invalid.date.pattern",
                    "The parameter date (" + stringDate + ") is invalid w.r.t. pattern " + pattern, "date", stringDate, pattern);
            dataValidationErrors.add(error);
            throw new PlatformApiDataValidationException("validation.msg.validation.errors.exist", "Validation errors exist.",
                    dataValidationErrors, e);
        }
    }

    public static String formatToSqlDate(final Date date) {
        final DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        df.setTimeZone(getTimeZoneOfTenant());
        final String formattedSqlDate = df.format(date);
        return formattedSqlDate;
    }

    public static boolean isDateInTheFuture(final LocalDate localDate) {
        return localDate.isAfter(getLocalDateOfTenant());
    }

    public static Date createDate(final String value, final String format) {
        Date date = null;    
        try {
            date = new SimpleDateFormat(format).parse(value);
        } catch (ParseException e) {
            final List<ApiParameterError> dataValidationErrors = new ArrayList<>();
            throw new PlatformApiDataValidationException("validation.msg.validation.errors.exist", e.getMessage(),
            dataValidationErrors, e);
        }
        return date;
    } 

    public static String formatDate(final Date date, final String format) {
        final DateFormat df = new SimpleDateFormat(format);
        df.setTimeZone(getTimeZoneOfTenant());
        final String formattedSqlDate = df.format(date);
        return formattedSqlDate;
    }

    public static Long getDaysInBetween(final Date date1, final Date date2) {
        if (date1 == null || date2 == null) {
            return 0L;
        }
        final Long diff = date1.getTime() - date2.getTime();
        return TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS);
    }
}

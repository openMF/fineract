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
package org.apache.fineract.commands.data.request;

import jakarta.ws.rs.QueryParam;
import java.io.Serial;
import java.io.Serializable;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Locale;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.fineract.infrastructure.core.serialization.JsonParserHelper;
import org.apache.fineract.infrastructure.core.service.DateUtils;

@Setter
@Getter
@NoArgsConstructor
public class AuditRequest implements Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    @QueryParam("actionName")
    private String actionName;
    @QueryParam("entityName")
    private String entityName;
    @QueryParam("resourceId")
    private Long resourceId;
    @QueryParam("subresourceId")
    private Long subresourceId;
    @QueryParam("resourceIdentifier")
    private String resourceIdentifier;
    @QueryParam("makerId")
    private Long makerId;
    @QueryParam("makerDateTimeFrom")
    private String makerDateFromString;
    @QueryParam("makerDateTimeTo")
    private String makerDateToString;
    @QueryParam("checkerId")
    private Long checkerId;
    @QueryParam("checkerDateTimeFrom")
    private String checkerDateFromString;
    @QueryParam("checkerDateTimeTo")
    private String checkerDateToString;
    @QueryParam("status")
    private String status;
    @QueryParam("clientId")
    private Long clientId;
    @QueryParam("loanId")
    private Long loanId;
    @QueryParam("officeId")
    private Long officeId;
    @QueryParam("groupId")
    private Long groupId;
    @QueryParam("savingsAccountId")
    private Long savingsAccountId;
    @QueryParam("transactionId")
    private String transactionId;
    @QueryParam("locale")
    private String localeString;
    @QueryParam("dateFormat")
    private String dateFormat;

    public Locale getLocale() {
        return localeString == null ? null : JsonParserHelper.localeFromString(localeString);
    }

    public OffsetDateTime getMakerDateTimeFrom() {
        return makerDateFromString == null ? null
                : DateUtils.parseLocalDate(makerDateFromString, dateFormat, getLocale()).atStartOfDay(ZoneOffset.UTC).toOffsetDateTime();
    }

    public OffsetDateTime getMakerDateTimeTo() {
        return makerDateToString == null ? null
                : DateUtils.parseLocalDate(makerDateToString, dateFormat, getLocale()).atStartOfDay(ZoneOffset.UTC).toOffsetDateTime();
    }

    public OffsetDateTime getCheckerDateTimeFrom() {
        return checkerDateFromString == null ? null
                : DateUtils.parseLocalDate(checkerDateFromString, dateFormat, getLocale()).atStartOfDay(ZoneOffset.UTC).toOffsetDateTime();
    }

    public OffsetDateTime getCheckerDateTimeTo() {
        return checkerDateToString == null ? null
                : DateUtils.parseLocalDate(checkerDateToString, dateFormat, getLocale()).atStartOfDay(ZoneOffset.UTC).toOffsetDateTime();
    }
}

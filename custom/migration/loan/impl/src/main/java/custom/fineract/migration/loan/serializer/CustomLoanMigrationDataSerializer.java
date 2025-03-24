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
package custom.fineract.migration.loan.serializer;

import custom.fineract.migration.loan.service.MigrationService;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import lombok.RequiredArgsConstructor;
import org.apache.fineract.infrastructure.event.business.domain.loan.LoanBusinessEvent;
import org.apache.fineract.infrastructure.event.external.service.serialization.serializer.ExternalEventCustomDataSerializer;
import org.apache.fineract.infrastructure.event.external.service.support.ByteBufferConverter;

@RequiredArgsConstructor
public class CustomLoanMigrationDataSerializer implements ExternalEventCustomDataSerializer<LoanBusinessEvent> {

    private final MigrationService migrationService;
    private final ByteBufferConverter byteBufferConverter;

    @Override
    public ByteBuffer serialize(final LoanBusinessEvent event) {
        final Long loanId = event.get().getId();

        return migrationService.findLastStatus(loanId) //
                .map(s -> byteBufferConverter.convert(s.getBytes(StandardCharsets.UTF_8))) //
                .orElse(null);
    }

    @Override
    public String key() {
        return "lastLoanMigrationStatus";
    }

}

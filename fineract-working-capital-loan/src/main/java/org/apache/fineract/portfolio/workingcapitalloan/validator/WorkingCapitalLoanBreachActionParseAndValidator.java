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
package org.apache.fineract.portfolio.workingcapitalloan.validator;

import static org.apache.fineract.portfolio.workingcapitalloan.validator.WorkingCapitalLoanBreachActionParameters.ACTION;
import static org.apache.fineract.portfolio.workingcapitalloan.validator.WorkingCapitalLoanBreachActionParameters.FREQUENCY;
import static org.apache.fineract.portfolio.workingcapitalloan.validator.WorkingCapitalLoanBreachActionParameters.FREQUENCY_TYPE;
import static org.apache.fineract.portfolio.workingcapitalloan.validator.WorkingCapitalLoanBreachActionParameters.MINIMUM_PAYMENT;
import static org.apache.fineract.portfolio.workingcapitalloan.validator.WorkingCapitalLoanBreachActionParameters.MINIMUM_PAYMENT_TYPE;

import com.google.gson.JsonElement;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.apache.fineract.infrastructure.core.api.JsonCommand;
import org.apache.fineract.infrastructure.core.data.DataValidatorBuilder;
import org.apache.fineract.infrastructure.core.serialization.FromJsonHelper;
import org.apache.fineract.infrastructure.core.service.DateUtils;
import org.apache.fineract.infrastructure.core.validator.ParseAndValidator;
import org.apache.fineract.portfolio.workingcapitalloan.domain.WorkingCapitalLoan;
import org.apache.fineract.portfolio.workingcapitalloan.domain.WorkingCapitalLoanBreachAction;
import org.apache.fineract.portfolio.workingcapitalloan.domain.WorkingCapitalLoanBreachActionType;
import org.apache.fineract.portfolio.workingcapitalloan.domain.WorkingCapitalLoanBreachSchedule;
import org.apache.fineract.portfolio.workingcapitalloan.domain.WorkingCapitalLoanDisbursementDetails;
import org.apache.fineract.portfolio.workingcapitalloan.domain.WorkingCapitalLoanPeriodFrequencyType;
import org.apache.fineract.portfolio.workingcapitalloan.repository.WorkingCapitalLoanBreachScheduleRepository;
import org.apache.fineract.portfolio.workingcapitalloanproduct.domain.WorkingCapitalBreachAmountCalculationType;
import org.springframework.stereotype.Component;

@RequiredArgsConstructor
@Component
public class WorkingCapitalLoanBreachActionParseAndValidator extends ParseAndValidator {

    private static final String RESCHEDULE_ACTION = "reschedule";

    private final FromJsonHelper jsonHelper;
    private final WorkingCapitalLoanBreachScheduleRepository breachScheduleRepository;

    public WorkingCapitalLoanBreachAction validateAndParse(final JsonCommand command, final WorkingCapitalLoan workingCapitalLoan) {
        final DataValidatorBuilder dataValidator = new DataValidatorBuilder(new ArrayList<>()).resource("workingCapitalLoanBreachAction");
        final WorkingCapitalLoanBreachAction parsedAction = parseCommand(command, dataValidator);
        validateLoanIsActive(workingCapitalLoan, dataValidator);

        if (WorkingCapitalLoanBreachActionType.RESCHEDULE.equals(parsedAction.getAction())) {
            validateReschedule(parsedAction, workingCapitalLoan, dataValidator);
        } else if (parsedAction.getAction() != null) {
            dataValidator.reset().parameter(ACTION).value(parsedAction.getAction()).failWithCode("invalid.action");
        }

        throwExceptionIfValidationWarningsExist(dataValidator);
        return parsedAction;
    }

    private WorkingCapitalLoanBreachAction parseCommand(final JsonCommand command, final DataValidatorBuilder dataValidator) {
        final JsonElement json = command.parsedJson();
        final WorkingCapitalLoanBreachAction action = new WorkingCapitalLoanBreachAction();
        action.setAction(extractAction(json, dataValidator));
        action.setStartDate(DateUtils.getBusinessLocalDate());
        action.setMinimumPayment(extractBigDecimal(json, MINIMUM_PAYMENT));
        action.setMinimumPaymentType(extractMinimumPaymentType(json, dataValidator));
        action.setFrequency(extractInteger(json, FREQUENCY));
        action.setFrequencyType(extractFrequencyType(json, dataValidator));
        return action;
    }

    private WorkingCapitalLoanBreachActionType extractAction(final JsonElement json, final DataValidatorBuilder dataValidator) {
        final String actionString = jsonHelper.extractStringNamed(ACTION, json);
        dataValidator.reset().parameter(ACTION).value(actionString).notBlank();
        if (StringUtils.isNotBlank(actionString)) {
            dataValidator.reset().parameter(ACTION).value(actionString).isOneOfTheseStringValues(RESCHEDULE_ACTION);
        }
        if (RESCHEDULE_ACTION.equalsIgnoreCase(actionString)) {
            return WorkingCapitalLoanBreachActionType.RESCHEDULE;
        }
        return null;
    }

    private BigDecimal extractBigDecimal(final JsonElement json, final String paramName) {
        if (json.getAsJsonObject().has(paramName)) {
            return jsonHelper.extractBigDecimalWithLocaleNamed(paramName, json);
        }
        return null;
    }

    private Integer extractInteger(final JsonElement json, final String paramName) {
        if (json.getAsJsonObject().has(paramName)) {
            return jsonHelper.extractIntegerWithLocaleNamed(paramName, json);
        }
        return null;
    }

    private WorkingCapitalBreachAmountCalculationType extractMinimumPaymentType(final JsonElement json,
            final DataValidatorBuilder dataValidator) {
        final String value = jsonHelper.extractStringNamed(MINIMUM_PAYMENT_TYPE, json);
        if (StringUtils.isEmpty(value)) {
            return null;
        }
        try {
            return WorkingCapitalBreachAmountCalculationType.valueOf(value.toUpperCase());
        } catch (IllegalArgumentException e) {
            dataValidator.reset().parameter(MINIMUM_PAYMENT_TYPE).value(value).failWithCode("invalid.minimumPaymentType");
            return null;
        }
    }

    private WorkingCapitalLoanPeriodFrequencyType extractFrequencyType(final JsonElement json, final DataValidatorBuilder dataValidator) {
        final String value = jsonHelper.extractStringNamed(FREQUENCY_TYPE, json);
        if (StringUtils.isEmpty(value)) {
            return null;
        }
        try {
            return WorkingCapitalLoanPeriodFrequencyType.valueOf(value.toUpperCase());
        } catch (IllegalArgumentException e) {
            dataValidator.reset().parameter(FREQUENCY_TYPE).value(value).failWithCode("invalid.frequencyType");
            return null;
        }
    }

    private void validateReschedule(final WorkingCapitalLoanBreachAction action, final WorkingCapitalLoan workingCapitalLoan,
            final DataValidatorBuilder dataValidator) {
        validateLoanIsDisbursed(workingCapitalLoan, dataValidator);
        validateScheduleExists(workingCapitalLoan, dataValidator);
        validateBreachConfigured(workingCapitalLoan, dataValidator);

        final boolean hasPaymentGroup = action.getMinimumPayment() != null || action.getMinimumPaymentType() != null;
        final boolean hasFrequencyGroup = action.getFrequency() != null || action.getFrequencyType() != null;

        if (!hasPaymentGroup && !hasFrequencyGroup) {
            dataValidator.reset().failWithCodeNoParameterAddedToErrorCode("reschedule.no.change.parameters");
        }
        if (hasPaymentGroup) {
            validateMinimumPaymentGroupProvided(action, dataValidator);
        }
        if (hasFrequencyGroup) {
            validateFrequencyGroupProvided(action, dataValidator);
        }
    }

    private void validateLoanIsActive(final WorkingCapitalLoan workingCapitalLoan, final DataValidatorBuilder dataValidator) {
        if (!workingCapitalLoan.getLoanStatus().isActive()) {
            dataValidator.reset().failWithCodeNoParameterAddedToErrorCode("loan.is.not.active");
        }
    }

    private void validateLoanIsDisbursed(final WorkingCapitalLoan workingCapitalLoan, final DataValidatorBuilder dataValidator) {
        final boolean isDisbursed = workingCapitalLoan.getDisbursementDetails().stream()
                .map(WorkingCapitalLoanDisbursementDetails::getActualDisbursementDate).anyMatch(Objects::nonNull);
        if (!isDisbursed) {
            dataValidator.reset().failWithCodeNoParameterAddedToErrorCode("loan.not.disbursed");
        }
    }

    private void validateScheduleExists(final WorkingCapitalLoan workingCapitalLoan, final DataValidatorBuilder dataValidator) {
        final List<WorkingCapitalLoanBreachSchedule> periods = breachScheduleRepository
                .findByLoanIdOrderByPeriodNumberAsc(workingCapitalLoan.getId());
        if (periods.isEmpty()) {
            dataValidator.reset().failWithCodeNoParameterAddedToErrorCode("no.breach.schedule");
        }
    }

    private void validateBreachConfigured(final WorkingCapitalLoan workingCapitalLoan, final DataValidatorBuilder dataValidator) {
        if (workingCapitalLoan.getLoanProductRelatedDetails() == null
                || workingCapitalLoan.getLoanProductRelatedDetails().getBreach() == null) {
            dataValidator.reset().failWithCodeNoParameterAddedToErrorCode("no.breach.configuration");
        }
    }

    private void validateMinimumPaymentGroupProvided(final WorkingCapitalLoanBreachAction action,
            final DataValidatorBuilder dataValidator) {
        if (action.getMinimumPayment() == null || action.getMinimumPayment().compareTo(BigDecimal.ZERO) <= 0) {
            dataValidator.reset().parameter(MINIMUM_PAYMENT).value(action.getMinimumPayment()).failWithCode("must.be.greater.than.zero");
        }
        if (action.getMinimumPaymentType() == null) {
            dataValidator.reset().parameter(MINIMUM_PAYMENT_TYPE).value(action.getMinimumPaymentType()).notNull();
        }
    }

    private void validateFrequencyGroupProvided(final WorkingCapitalLoanBreachAction action, final DataValidatorBuilder dataValidator) {
        if (action.getFrequency() == null || action.getFrequency() <= 0) {
            dataValidator.reset().parameter(FREQUENCY).value(action.getFrequency()).integerGreaterThanZero();
        }
        if (action.getFrequencyType() == null) {
            dataValidator.reset().parameter(FREQUENCY_TYPE).value(action.getFrequencyType()).notNull();
        }
    }
}

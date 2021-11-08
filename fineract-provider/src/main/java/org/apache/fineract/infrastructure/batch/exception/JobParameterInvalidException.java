package org.apache.fineract.infrastructure.batch.exception;

import org.apache.fineract.infrastructure.core.exception.AbstractPlatformDomainRuleException;

public class JobParameterInvalidException extends AbstractPlatformDomainRuleException {

    public JobParameterInvalidException() {
        super("error.msg.batch.job.parameter.invalid", "Batch job with invalid parameters");
    }
}

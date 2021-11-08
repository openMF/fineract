package org.apache.fineract.infrastructure.batch.exception;

import org.apache.fineract.infrastructure.core.exception.AbstractPlatformDomainRuleException;

public class JobIllegalRestartException extends AbstractPlatformDomainRuleException {

    public JobIllegalRestartException() {
        super("error.msg.batch.job.restar", "Batch job with illegal problem at restart");
    }
}

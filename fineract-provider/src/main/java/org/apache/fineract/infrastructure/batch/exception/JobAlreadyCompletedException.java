package org.apache.fineract.infrastructure.batch.exception;

import org.apache.fineract.infrastructure.core.exception.AbstractPlatformDomainRuleException;

public class JobAlreadyCompletedException extends AbstractPlatformDomainRuleException {

    public JobAlreadyCompletedException() {
        super("error.msg.batch.job.already.completed", "This batch job is already completed");
    }
}

package org.apache.fineract.infrastructure.batch.exception;

import org.apache.fineract.infrastructure.core.exception.AbstractPlatformDomainRuleException;

public class JobAlreadyRunningException extends AbstractPlatformDomainRuleException {

    public JobAlreadyRunningException() {
        super("error.msg.batch.job.already.running", "This batch job is already running");
    }
}

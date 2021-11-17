package org.apache.fineract.infrastructure.batch.service;

public interface JobRunner {

    Long runJob(final Long jobId);

    Long runJob(final Long jobId, final String parameter);

    void stopJob(final Long jobId);
}

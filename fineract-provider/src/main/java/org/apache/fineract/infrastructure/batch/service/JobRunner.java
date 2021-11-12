package org.apache.fineract.infrastructure.batch.service;

public interface JobRunner {

    Long runJob(final Long jobId);

    void stopJob(final Long jobId);
}

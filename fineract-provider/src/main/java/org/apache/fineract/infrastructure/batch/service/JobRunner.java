package org.apache.fineract.infrastructure.batch.service;

public interface JobRunner {

    void runJob(final Long jobId);

    void stopJob(final Long jobId);
}

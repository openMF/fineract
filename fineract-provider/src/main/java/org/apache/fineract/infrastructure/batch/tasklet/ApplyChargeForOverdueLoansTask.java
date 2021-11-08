package org.apache.fineract.infrastructure.batch.tasklet;

import java.util.Collection;
import org.apache.fineract.infrastructure.configuration.domain.ConfigurationDomainService;
import org.apache.fineract.portfolio.loanaccount.loanschedule.data.OverdueLoanScheduleData;
import org.apache.fineract.portfolio.loanaccount.service.LoanReadPlatformService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;

public class ApplyChargeForOverdueLoansTask implements Tasklet {

    private static final Logger LOG = LoggerFactory.getLogger(ApplyChargeForOverdueLoansTask.class);

    @Autowired
    private ConfigurationDomainService configurationDomainService;

    @Autowired
    private LoanReadPlatformService loanReadPlatformService;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        LOG.info("Task: " + ApplyChargeForOverdueLoansTask.class);

        final Long penaltyWaitPeriodValue = this.configurationDomainService.retrievePenaltyWaitPeriod();
        final Boolean backdatePenalties = this.configurationDomainService.isBackdatePenaltiesEnabled();
        final Collection<OverdueLoanScheduleData> overdueLoanScheduledInstallments = this.loanReadPlatformService
                .retrieveAllLoansWithOverdueInstallments(penaltyWaitPeriodValue, backdatePenalties);
        LOG.info("penaltyWaitPeriodValue : " + penaltyWaitPeriodValue);
        LOG.info("backdatePenalties      : " + backdatePenalties);

        if (!overdueLoanScheduledInstallments.isEmpty()) {
            LOG.info("Items: " + overdueLoanScheduledInstallments.size());
        }
        LOG.info("Task finish!");
        return RepeatStatus.FINISHED;
    }

}

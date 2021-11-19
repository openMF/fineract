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
package org.apache.fineract.infrastructure.batch.config;

import java.util.Properties;
import javax.annotation.PostConstruct;
import javax.sql.DataSource;

import org.apache.fineract.infrastructure.batch.data.MessageBatchDataResponse;
import org.apache.fineract.infrastructure.batch.listeners.ItemCounterListener;
import org.apache.fineract.infrastructure.batch.processor.ApplyChargeForOverdueLoansProcessor;
import org.apache.fineract.infrastructure.batch.processor.AutopayLoansProcessor;
import org.apache.fineract.infrastructure.batch.processor.BatchLoansProcessor;
import org.apache.fineract.infrastructure.batch.reader.ApplyChargeForOverdueLoansReader;
import org.apache.fineract.infrastructure.batch.reader.AutopayLoansReader;
import org.apache.fineract.infrastructure.batch.reader.BatchLoansReader;
import org.apache.fineract.infrastructure.batch.writer.ApplyChargeForOverdueLoansWriter;
import org.apache.fineract.infrastructure.batch.writer.AutopayLoansWriter;
import org.apache.fineract.infrastructure.batch.writer.BatchLoansWriter;
import org.apache.fineract.infrastructure.core.service.RoutingDataSourceService;
import org.apache.fineract.infrastructure.core.utils.DatabaseUtils;
import org.apache.fineract.infrastructure.core.utils.PropertyUtils;
import org.apache.fineract.infrastructure.jobs.data.JobConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.BatchConfigurationException;
import org.springframework.batch.core.configuration.annotation.DefaultBatchConfigurer;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.explore.support.JobExplorerFactoryBean;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.context.annotation.Scope;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;


@Profile(JobConstants.SPRING_BATCH_PROFILE_NAME)
@Configuration
@EnableBatchProcessing
public class BatchConfiguration extends DefaultBatchConfigurer {

    public static final Logger LOG = LoggerFactory.getLogger(BatchConfiguration.class);

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    private final Properties batchJobProperties;
    private String databaseType;
    private RoutingDataSourceService routingDataSourceService;
    private PlatformTransactionManager transactionManager;
    private JobRepository jobRepository;
    private JobLauncher jobLauncher;
    private JobExplorer jobExplorer;

    @Autowired
    private ApplicationContext context;

    public BatchConfiguration(RoutingDataSourceService tomcatJdbcDataSourcePerTenantService) {
        routingDataSourceService = tomcatJdbcDataSourcePerTenantService;
        batchJobProperties = PropertyUtils.loadYamlProperties(BatchConstants.BATCH_PROPERTIES_FILE);
        this.databaseType = DatabaseUtils.getDatabaseType(getDataSource());
    }

    @PostConstruct
    @Override
    public void initialize() {
        try {
            this.jobRepository = createJobRepository();
            this.jobExplorer = createJobExplorer();
            this.jobLauncher = createJobLauncher();
        } catch (Exception e) {
            throw new BatchConfigurationException(e);
        }
    }

    @Override
    public JobRepository getJobRepository() {
        return jobRepository;
    }

    @Override
    public PlatformTransactionManager getTransactionManager() {
        return new DataSourceTransactionManager(getDataSource());
    }

    @Override
    public JobLauncher getJobLauncher() {
        return jobLauncher;
    }

    @Override
    public JobExplorer getJobExplorer() {
        return jobExplorer;
    }

    @Override
    protected JobLauncher createJobLauncher() throws Exception {
        SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
        jobLauncher.setJobRepository(jobRepository);
        jobLauncher.afterPropertiesSet();
        return jobLauncher;
    }

    protected DataSource getDataSource() {
        return routingDataSourceService.retrieveDataSource();
    }

    @Override
    protected JobRepository createJobRepository() throws Exception {
        JobRepositoryFactoryBean factory = new JobRepositoryFactoryBean();
        factory.setDataSource(getDataSource());
        factory.setTransactionManager(getTransactionManager());
        factory.afterPropertiesSet();
        return factory.getObject();
    }

    @Override
    protected JobExplorer createJobExplorer() throws Exception {
        JobExplorerFactoryBean jobExplorerFactoryBean = new JobExplorerFactoryBean();
        jobExplorerFactoryBean.setDataSource(this.getDataSource());
        jobExplorerFactoryBean.afterPropertiesSet();
        return jobExplorerFactoryBean.getObject();
    }

    @Bean
    public Properties batchJobProperties() {
        return this.batchJobProperties;
    }

    @Bean
    @Scope("prototype")
    public JobRepository batchJobRepository() {
        try {
            JobRepositoryFactoryBean factory = new JobRepositoryFactoryBean();
            factory.setDatabaseType(databaseType);
            factory.setTransactionManager(getTransactionManager());
            factory.setDataSource(this.getDataSource());
            factory.afterPropertiesSet();
            return factory.getObject();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @Bean(name = "batchJobLauncher")
    @Scope("prototype")
    public JobLauncher batchJobLauncher() {
        try {
            ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
            // The number of concurrent executions is 3.Wait for more queues.
            taskExecutor.setCorePoolSize(3); // (4)
            // java.lang.IllegalStateException: ThreadPoolTaskExecutor not initialized
            taskExecutor.initialize(); // (5)

            SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
            jobLauncher.setJobRepository(batchJobRepository());
            jobLauncher.setTaskExecutor(taskExecutor);

            return jobLauncher;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @Bean
    public BatchDestinations batchDestinations() {
        return new BatchDestinations(batchJobProperties(), this.context.getEnvironment());
    }

    @Bean
    public JobExecutionListener jobExecutionListener() {
        return new JobExecutionListener() {

            @Override
            public void beforeJob(JobExecution jobExecution) {
                String jobName = jobExecution.getJobInstance().getJobName();
                LOG.info("About to execute the job : {}", jobName);
            }

            @Override
            public void afterJob(JobExecution jobExecution) {
                String jobName = jobExecution.getJobInstance().getJobName();
                LOG.info("Finished execution {} {}", jobName, jobExecution.getExitStatus().getExitDescription());
            }
        };
    }

    @Bean
    public ItemCounterListener itemCounterListener() {
        return new ItemCounterListener();
    }

    // Readers
    @Bean
    public BatchLoansReader batchLoansReader() {
        return new BatchLoansReader();
    }

    @Bean
    public AutopayLoansReader autopayLoansReader() {
        return new AutopayLoansReader();
    }

    @Bean
    public ApplyChargeForOverdueLoansReader applyChargeForOverdueLoansReader() {
        return new ApplyChargeForOverdueLoansReader();
    }

    // Processors
    @Bean
    public BatchLoansProcessor batchLoansProcessor() {
        return new BatchLoansProcessor();
    }

    @Bean
    public AutopayLoansProcessor autopayLoansProcessor() {
        return new AutopayLoansProcessor();
    }

    @Bean
    public ApplyChargeForOverdueLoansProcessor applyChargeForOverdueLoansProcessor() {
        return new ApplyChargeForOverdueLoansProcessor();
    }

    // Writers
    @Bean
    public BatchLoansWriter batchLoansWriter() {
        return new BatchLoansWriter(batchDestinations());
    }

    @Bean
    public AutopayLoansWriter autopayLoansWriter() {
        return new AutopayLoansWriter(batchDestinations());
    }

    @Bean
    public ApplyChargeForOverdueLoansWriter applyChargeForOverdueLoansWriter() {
        return new ApplyChargeForOverdueLoansWriter(batchDestinations());
    }

    // Steps
    @Bean
    public Step batchForLoansStep(BatchLoansReader batchLoansReader, BatchLoansProcessor batchLoansProcessor,
            BatchLoansWriter batchLoansWriter) {
        final int chunkSize = Integer.parseInt(this.batchJobProperties.getProperty("fineract.batch.jobs.chunk.size", "1000"));
        Step step = stepBuilderFactory.get("applyChargeForOverdueLoansStep").<Long, Long>chunk(chunkSize).reader(batchLoansReader)
                .processor(batchLoansProcessor).writer(batchLoansWriter).listener(itemCounterListener()).build();

        return step;
    }

    @Bean
    public Step autopayLoansStep(AutopayLoansReader autopayLoansReader, AutopayLoansProcessor autopayLoansProcessor,
            AutopayLoansWriter autopayLoansWriter) {
        final int chunkSize = Integer.parseInt(this.batchJobProperties.getProperty("fineract.batch.jobs.chunk.size", "1000"));
        Step step = stepBuilderFactory.get("autopayLoansStep").<Long, Long>chunk(chunkSize).reader(autopayLoansReader)
                .processor(autopayLoansProcessor).writer(autopayLoansWriter).listener(itemCounterListener()).build();

        return step;
    }

    @Bean
    public Step applyChargeForOverdueLoansStep(ApplyChargeForOverdueLoansReader applyChargeForOverdueLoansReader,
            ApplyChargeForOverdueLoansProcessor applyChargeForOverdueLoansProcessor,
            ApplyChargeForOverdueLoansWriter applyChargeForOverdueLoansWriter) {
        final int chunkSize = 10;
        Step step = stepBuilderFactory.get("applyChargeForOverdueLoansStep")
                .<Long, MessageBatchDataResponse>chunk(chunkSize).reader(applyChargeForOverdueLoansReader)
                .processor(applyChargeForOverdueLoansProcessor).writer(applyChargeForOverdueLoansWriter).listener(itemCounterListener())
                .build();

        return step;
    }
}

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
import org.apache.fineract.infrastructure.batch.processor.TaggingLoansProcessor;
import org.apache.fineract.infrastructure.batch.reader.BatchLoansReader;
import org.apache.fineract.infrastructure.batch.reader.BlockLoansReader;
import org.apache.fineract.infrastructure.batch.writer.ApplyChargeForOverdueLoansWriter;
import org.apache.fineract.infrastructure.batch.writer.AutopayLoansWriter;
import org.apache.fineract.infrastructure.batch.writer.BatchLoansWriter;
import org.apache.fineract.infrastructure.batch.writer.TaggingLoansWriter;
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
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
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
public class BatchConfiguration extends DefaultBatchConfigurer {

    public static final Logger LOG = LoggerFactory.getLogger(BatchConfiguration.class);

    @Autowired
    private ApplicationContext context;
    
    private String databaseType;

    private final Properties batchJobProperties;
    private PlatformTransactionManager transactionManager;
    private RoutingDataSourceService routingDataSourceService;
    private JobRepository jobRepository;
    private JobLauncher jobLauncher;
    private JobExplorer jobExplorer;
    private StepBuilderFactory stepBuilderFactory;

    public BatchConfiguration(RoutingDataSourceService tomcatJdbcDataSourcePerTenantService) {
        routingDataSourceService = tomcatJdbcDataSourcePerTenantService;
        batchJobProperties = PropertyUtils.loadYamlProperties(BatchConstants.BATCH_PROPERTIES_FILE);
        this.databaseType = DatabaseUtils.getDatabaseType(getDataSource());
        this.transactionManager = new DataSourceTransactionManager(getDataSource());
    }

    @PostConstruct
    @Override
    public void initialize() {
        try {
            this.jobRepository = createJobRepository();
            this.jobExplorer = createJobExplorer();
            this.jobLauncher = createJobLauncher();
            this.stepBuilderFactory = createStepBuilderFactory();
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
        return this.transactionManager;
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

    protected StepBuilderFactory createStepBuilderFactory() {
        return new StepBuilderFactory(this.jobRepository, this.transactionManager);
    }

    @Override
    protected JobExplorer createJobExplorer() throws Exception {
        JobExplorerFactoryBean jobExplorerFactoryBean = new JobExplorerFactoryBean();
        jobExplorerFactoryBean.setDataSource(this.getDataSource());
        jobExplorerFactoryBean.afterPropertiesSet();
        return jobExplorerFactoryBean.getObject();
    }
    
    @Bean
    public JobBuilderFactory jobBuilderFactory() {
        return new JobBuilderFactory(this.jobRepository);
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
    public BlockLoansReader blockLoansReader() {
        return new BlockLoansReader();
    }

    // Processors
    @Bean
    public AutopayLoansProcessor autopayLoansProcessor() {
        return new AutopayLoansProcessor();
    }

    @Bean
    public ApplyChargeForOverdueLoansProcessor applyChargeForOverdueLoansProcessor() {
        return new ApplyChargeForOverdueLoansProcessor();
    }

    @Bean
    public TaggingLoansProcessor taggingLoansProcessor() {
        return new TaggingLoansProcessor();
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

    @Bean
    public TaggingLoansWriter taggingLoansWriter() {
        return new TaggingLoansWriter(batchDestinations());
    }

    // Steps
    @Bean
    public Step batchForLoansStep(BatchLoansReader batchLoansReader,
            BatchLoansWriter batchLoansWriter) {
        final int chunkSize = Integer.parseInt(this.batchJobProperties.getProperty("fineract.batch.jobs.chunk.size", "1000"));
        Step step = stepBuilderFactory.get("batchForLoansStep").<Long, Long>chunk(chunkSize).reader(batchLoansReader)
                .writer(batchLoansWriter).listener(itemCounterListener()).build();

        return step;
    }

    @Bean
    public Step autopayLoansStep(BlockLoansReader blockLoansReader, AutopayLoansProcessor autopayLoansProcessor,
            AutopayLoansWriter autopayLoansWriter) {
        final int chunkSize = Integer.parseInt(this.batchJobProperties.getProperty("fineract.batch.jobs.chunk.size", "1000"));
        Step step = stepBuilderFactory.get("autopayLoansStep")
                .<Long, MessageBatchDataResponse>chunk(chunkSize).reader(blockLoansReader)
                .processor(autopayLoansProcessor).writer(autopayLoansWriter).listener(itemCounterListener())
                .build();

        return step;
    }

    @Bean
    public Step applyChargeForOverdueLoansStep(BlockLoansReader blockLoansReader,
            ApplyChargeForOverdueLoansProcessor applyChargeForOverdueLoansProcessor,
            ApplyChargeForOverdueLoansWriter applyChargeForOverdueLoansWriter) {
        final int chunkSize = Integer.parseInt(this.batchJobProperties.getProperty("fineract.batch.jobs.chunk.size", "1000"));
        Step step = stepBuilderFactory.get("applyChargeForOverdueLoansStep")
                .<Long, MessageBatchDataResponse>chunk(chunkSize).reader(blockLoansReader)
                .processor(applyChargeForOverdueLoansProcessor).writer(applyChargeForOverdueLoansWriter).listener(itemCounterListener())
                .build();

        return step;
    }

    @Bean
    public Step taggingLoansStep(BlockLoansReader blockLoansReader,
        TaggingLoansProcessor taggingLoansProcessor, TaggingLoansWriter taggingLoansWriter) {
        final int chunkSize = Integer.parseInt(this.batchJobProperties.getProperty("fineract.batch.jobs.chunk.size", "1000"));
        Step step = stepBuilderFactory.get("taggingLoansStep")
                .<Long, MessageBatchDataResponse>chunk(chunkSize).reader(blockLoansReader)
                .processor(taggingLoansProcessor).writer(taggingLoansWriter).listener(itemCounterListener())
                .build();

        return step;
    }

}
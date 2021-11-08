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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import javax.sql.DataSource;
import org.apache.fineract.infrastructure.batch.processor.ApplyChargeForOverdueLoansProcessor;
import org.apache.fineract.infrastructure.batch.reader.ApplyChargeForOverdueLoansReader;
import org.apache.fineract.infrastructure.batch.writer.ApplyChargeForOverdueLoansWriter;
import org.apache.fineract.infrastructure.core.utils.PropertyUtils;
import org.apache.fineract.portfolio.loanaccount.loanschedule.data.OverdueLoanScheduleData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.DefaultBatchConfigurer;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.init.DataSourceInitializer;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

@Profile("batch")
@Configuration
@EnableBatchProcessing
public class BatchConfiguration extends DefaultBatchConfigurer {

    public static final Logger LOG = LoggerFactory.getLogger(BatchConfiguration.class);

    public static final String BATCH_PROPERTIES = "/batch.yml";

    private final DataSource dataSource;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private JobRepository jobRepository;

    private final String databaseType;

    public BatchConfiguration(final DataSource dataSource) {
        this.dataSource = dataSource;
        super.setDataSource(this.dataSource);
        this.databaseType = getDatabaseType(dataSource);
        LOG.info("Database Type for Batch configuration {}", databaseType);
    }

    private String getDatabaseType(DataSource dataSource) {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        return jdbcTemplate.execute(new ConnectionCallback<String>() {

            @Override
            public String doInConnection(Connection connection) throws SQLException, DataAccessException {
                return connection.getMetaData().getDatabaseProductName().toLowerCase();
            }
        });
    }

    @Bean
    public DataSourceInitializer dataSourceInitializer(DataSource dataSource) {
        ResourceDatabasePopulator databasePopulator = new ResourceDatabasePopulator();

        databasePopulator.addScript(new ClassPathResource("sql/migrations/batch/schema-drop-" + databaseType + ".sql"));
        databasePopulator.addScript(new ClassPathResource("sql/migrations/batch/schema-" + databaseType + ".sql"));
        databasePopulator.setIgnoreFailedDrops(true);

        DataSourceInitializer initializer = new DataSourceInitializer();
        initializer.setDataSource(dataSource);
        initializer.setDatabasePopulator(databasePopulator);
        initializer.setEnabled(true);
        return initializer;
    }

    public JobRepository batchJobRepository() {
        try {
            JobRepositoryFactoryBean factory = new JobRepositoryFactoryBean();
            factory.setDataSource(this.dataSource);
            factory.setTransactionManager(getTxManager());
            if (!databaseType.isEmpty()) factory.setDatabaseType(databaseType);
            factory.afterPropertiesSet();
            return factory.getObject();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private PlatformTransactionManager getTxManager() {
        return new ResourcelessTransactionManager();
    }

    @Bean
    public JobLauncher batchJobLauncher() {
        try {
            ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
            // The number of concurrent executions is 3.Wait for more queues.
            taskExecutor.setCorePoolSize(3); // (4)
            // java.lang.IllegalStateException: ThreadPoolTaskExecutor not initialized
            taskExecutor.initialize(); // (5)

            SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
            jobLauncher.setJobRepository(jobRepository);
            jobLauncher.setTaskExecutor(taskExecutor);

            return jobLauncher;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @Bean
    public JobExecutionListener jobExecutionListener() {
        return new JobExecutionListener() {

            @Override
            public void beforeJob(JobExecution jobExecution) {}

            @Override
            public void afterJob(JobExecution jobExecution) {}
        };
    }

    // Readers
    @Bean
    public ApplyChargeForOverdueLoansReader applyChargeForOverdueLoansReader() {
        return new ApplyChargeForOverdueLoansReader();
    }

    // Processors
    @Bean
    public ApplyChargeForOverdueLoansProcessor applyChargeForOverdueLoansProcessor() {
        return new ApplyChargeForOverdueLoansProcessor();
    }

    // Writers
    @Bean
    public ApplyChargeForOverdueLoansWriter applyChargeForOverdueLoansWriter() {
        return new ApplyChargeForOverdueLoansWriter();
    }

    // Steps
    @Bean
    public Step applyChargeForOverdueLoansStep(ApplyChargeForOverdueLoansReader applyChargeForOverdueLoansReader,
            ApplyChargeForOverdueLoansProcessor applyChargeForOverdueLoansProcessor,
            ApplyChargeForOverdueLoansWriter applyChargeForOverdueLoansWriter) {
        Properties batchJobProperties = PropertyUtils.loadYamlProperties(BATCH_PROPERTIES);

        final int chunkSize = Integer.parseInt(batchJobProperties.getProperty("fineract.batch.jobs.chunk.size"));
        TaskletStep step = stepBuilderFactory.get("applyChargeForOverdueLoansStep")
                .<OverdueLoanScheduleData, OverdueLoanScheduleData>chunk(chunkSize).reader(applyChargeForOverdueLoansReader)
                .processor(applyChargeForOverdueLoansProcessor).writer(applyChargeForOverdueLoansWriter).build();
        return step;
    }
}

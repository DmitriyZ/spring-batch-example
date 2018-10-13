package ru.zaets.home.springbatch.demo.onetableprocessing.tasklet;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.jsr.configuration.support.BatchPropertyContext;
import org.springframework.batch.core.jsr.step.BatchletStep;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.job.JobStep;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.support.PostgresPagingQueryProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import ru.zaets.home.springbatch.demo.onetableprocessing.Item;

import javax.sql.DataSource;
import java.util.Collections;

import static org.springframework.batch.item.database.Order.ASCENDING;

@Slf4j
@Configuration
public class TaskletBatchConfiguration {

    @Autowired
    public DataSource dataSource;

    @Autowired
    public NamedParameterJdbcTemplate jdbcTemplate;

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Autowired
    public JobRepository jobRepository;

    @Bean
    public JobLauncher taskletJobLauncher() {
        SimpleJobLauncher simpleJobLauncher = new SimpleJobLauncher();
        simpleJobLauncher.setJobRepository(jobRepository);

        return simpleJobLauncher;
    }

//    @Bean
//    public Job taskletJob(StepBuilderFactory stepBuilderFactory, Step taskletStep) {
//        log.info("Create Job Main Job");
//        return jobBuilderFactory.get("main_job")
//                .incrementer(new RunIdIncrementer())
//                .start(taskletStep)
//                .build();
//    }
//
//
//    @Bean
//    public Step taskletStep() {
//
//        return new BatchletStep("test", new BatchPropertyContext());
//    }

}

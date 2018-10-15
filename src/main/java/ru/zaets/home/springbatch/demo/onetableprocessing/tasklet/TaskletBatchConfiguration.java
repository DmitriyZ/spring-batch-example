package ru.zaets.home.springbatch.demo.onetableprocessing.tasklet;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import javax.sql.DataSource;

@Slf4j
@Configuration
public class TaskletBatchConfiguration {

    @Autowired
    public DataSource dataSource;

    @Autowired
    public NamedParameterJdbcTemplate jdbcTemplate;

    @Autowired
    public JobBuilderFactory jobs;

    @Autowired
    public StepBuilderFactory steps;

    @Autowired
    public JobRepository jobRepository;


    @Bean
    protected Step readLines() {
        return steps
                .get("readLines")
                .tasklet(linesReader())
                .build();
    }

    @Bean
    protected Step processLines() {
        return steps
                .get("processLines")
                .tasklet(linesProcessor())
                .build();
    }

    @Bean
    protected Step writeLines() {
        return steps
                .get("writeLines")
                .tasklet(linesWriter())
                .build();
    }

    @Bean
    public Job job() {
        return jobs
                .get("taskletsJob")
                .start(readLines())
                .next(processLines())
                .next(writeLines())
                .build();
    }

    @Bean
    protected LinesReader linesReader() {
        return new LinesReader();
    }

    @Bean
    protected LinesProcessor linesProcessor() {
        return new LinesProcessor();
    }


    @Bean
    protected LinesWriter linesWriter() {
        return new LinesWriter();
    }


}

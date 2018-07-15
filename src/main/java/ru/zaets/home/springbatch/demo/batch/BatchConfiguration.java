package ru.zaets.home.springbatch.demo.batch;

import com.google.common.base.Stopwatch;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.scope.context.StepContext;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import ru.zaets.home.springbatch.demo.services.Status;

import java.util.HashMap;
import java.util.Map;

import static ru.zaets.home.springbatch.demo.services.BatchStarter.BIND_KEY;

/**
 * Created by dzaets on 15.07.2018.
 * spring-batch-example
 */
@Slf4j
@Configuration
public class BatchConfiguration {
    @Autowired
    public NamedParameterJdbcTemplate jdbcTemplate;

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Autowired
    public JobRepository jobRepository;

    @Bean
    public JobLauncher asyncJobLauncher() {
        SimpleJobLauncher simpleJobLauncher = new SimpleJobLauncher();
        simpleJobLauncher.setJobRepository(jobRepository);

//        SimpleAsyncTaskExecutor simpleAsyncTaskExecutor = new SimpleAsyncTaskExecutor("geo-fence-");
//        simpleAsyncTaskExecutor.setConcurrencyLimit(3);
//        simpleJobLauncher.setTaskExecutor(simpleAsyncTaskExecutor);

        return simpleJobLauncher;
    }

    @Bean
    public Job mainJob() {
        log.info("Create Job Main Job");
        return jobBuilderFactory.get("main_job")
                .incrementer(new RunIdIncrementer())
                .start(
                        stepBuilderFactory.get("test").tasklet((contribution, chunkContext) -> {

                            final StepContext stepContext = chunkContext.getStepContext();
                            final Map<String, Object> jobParameters = chunkContext.getStepContext().getJobParameters();

                            final Long id = (Long) jobParameters.get(BIND_KEY);
                            final Map<String, Object> inProgressParams = new HashMap<>();
                            inProgressParams.put("newStatus", Status.IN_PROGRESS.ordinal());
                            inProgressParams.put("oldStatus", Status.NEW.ordinal());
                            inProgressParams.put("jobId", id);

                            final int inProgress = jdbcTemplate.update("UPDATE numbers " +
                                    "SET status=:newStatus " +
                                    "WHERE status=:oldStatus", inProgressParams);

                            log.info("Step [{}] job bind key id [{}]. Marked IN_PROGRESS {} rows", stepContext.getStepName(), id, inProgress);

                            if (inProgress == 0) {
                                log.info("No data for processing. Finish the job!");
                                contribution.setExitStatus(ExitStatus.NOOP);
                                return RepeatStatus.FINISHED;
                            }

                            Map<String, Object> p = new HashMap<>();
                            p.put("jobId", id);
                            int update = jdbcTemplate.update("INSERT INTO result (id, value) " +
                                    "SELECT  n.id, n.value " +
                                    "FROM    numbers as n " +
                                    "WHERE  n.status = 1 and MOD (n.value, 2) = 1;", p);

                            log.info("" + update);


                            final Map<String, Object> processedParams = new HashMap<>();
                            processedParams.put("newStatus", Status.PROCESSED.ordinal());
                            processedParams.put("oldStatus", Status.IN_PROGRESS.ordinal());
                            processedParams.put("jobId", id);
                            final int processed = jdbcTemplate.update("UPDATE numbers " +
                                    "SET status=:newStatus " +
                                    "WHERE status=:oldStatus", processedParams);

                            log.info("Step [{}] job bind key id [{}]. Marked PROCESSED {} rows", stepContext.getStepName(), id, processed);

                            return RepeatStatus.FINISHED;
                        })
                                .listener(jobExecutionListener())
                                .build()
                ).on("NOOP").end()
                .next(stepBuilderFactory.get("dumb").tasklet((contribution, chunkContext) -> {
                    return RepeatStatus.FINISHED;
                }).build())
                .end()
                .build();
    }


    public StepExecutionListener jobExecutionListener() {
        return new StepExecutionListener() {

            private Stopwatch stopwatch;

            @Override
            public void beforeStep(StepExecution stepExecution) {
                stopwatch = Stopwatch.createStarted();
            }

            @Override
            public ExitStatus afterStep(StepExecution stepExecution) {
                log.info("Step [{}] execution time: {}", stepExecution.getStepName(), stopwatch.stop());
                return null;
            }
        };
    }

}

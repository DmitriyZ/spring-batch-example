package ru.zaets.home.springbatch.demo.services;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by dzaets on 15.07.2018.
 * spring-batch-example
 */
@Slf4j
@Component
public class BatchStarter {
    public final static String BIND_KEY = "bind.job.id";

    private final JobLauncher jobLauncher;
    private final Job mainJob;

    private final JobOperator jobOperator;

    public BatchStarter(JobLauncher jobLauncher, Job mainJob, JobOperator jobOperator) {
        this.jobLauncher = jobLauncher;
        this.mainJob = mainJob;
        this.jobOperator = jobOperator;
    }


    @Scheduled(fixedRate = 5000, initialDelay = 2000)
    public void generate1() throws JobParametersInvalidException, JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException {

        final long bindKeyID = System.currentTimeMillis();
        log.info("Schedule batch processing with Bind Key ID {}", bindKeyID);

        Map<String, JobParameter> parameters = new HashMap<>();
        parameters.put(BIND_KEY, new JobParameter(bindKeyID));

        jobLauncher.run(mainJob, new JobParameters(parameters));

        log.info("End batch processing!");
    }
}

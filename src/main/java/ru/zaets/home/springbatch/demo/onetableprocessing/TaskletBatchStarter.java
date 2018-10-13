package ru.zaets.home.springbatch.demo.onetableprocessing;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Collections;

/**
 * Created by dzaets on 15.07.2018.
 * spring-batch-example
 */
//@Slf4j
//@Component
//public class TaskletBatchStarter {
//
//    private JobLauncher jobLauncher;
//    private final Job mainJob;
//
//
//    public TaskletBatchStarter(JobLauncher taskletJobLauncher, Job taskletJob) {
//        this.jobLauncher = taskletJobLauncher;
//        this.mainJob = taskletJob;
//    }
//
//
//    @Scheduled(fixedRate = 15000, initialDelay = 2000)
//    public void generate1() {
//
//        final long bindKeyID = System.currentTimeMillis();
//        log.info("Schedule batch processing with Bind Key ID {}", bindKeyID);
//
//
//        try {
//            jobLauncher.run(mainJob, new JobParameters(Collections.singletonMap("id", new JobParameter(Instant.now().toString()))));
//        } catch (JobExecutionAlreadyRunningException | JobRestartException
//                | JobInstanceAlreadyCompleteException | JobParametersInvalidException e) {
//            e.printStackTrace();
//        }
//
//        log.info("End batch processing!");
//    }
//}

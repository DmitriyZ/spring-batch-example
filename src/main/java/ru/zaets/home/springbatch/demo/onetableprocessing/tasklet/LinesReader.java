package ru.zaets.home.springbatch.demo.onetableprocessing.tasklet;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.util.FileUtils;
import org.springframework.batch.repeat.RepeatStatus;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class LinesReader implements Tasklet, StepExecutionListener {

    private List<String> lines;

    @Override
    public void beforeStep(StepExecution stepExecution) {
        lines = new ArrayList<>();
        log.debug("Lines Reader initialized.");
    }

    @Override
    public RepeatStatus execute(StepContribution stepContribution,
                                ChunkContext chunkContext) throws Exception {
        String line = "dkfj";
            lines.add(line);
            log.debug("Read line: " + line.toString());
        return RepeatStatus.FINISHED;
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        stepExecution
                .getJobExecution()
                .getExecutionContext()
                .put("lines", this.lines);
        log.debug("Lines Reader ended.");
        return ExitStatus.COMPLETED;
    }
}

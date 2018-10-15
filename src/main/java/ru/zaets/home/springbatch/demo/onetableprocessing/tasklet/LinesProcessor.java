package ru.zaets.home.springbatch.demo.onetableprocessing.tasklet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.repeat.RepeatStatus;
import ru.zaets.home.springbatch.demo.onetableprocessing.Item;

import java.util.List;

public class LinesProcessor implements Tasklet, StepExecutionListener {

    private final Logger logger = LoggerFactory.getLogger(LinesProcessor.class);

    private List<Item> lines;

    @Override
    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
        for (Item line : lines) {
            logger.info("Calculated age " + " for line " + line.toString());
        }
        return RepeatStatus.FINISHED;
    }

    @Override
    public void beforeStep(StepExecution stepExecution) {
        ExecutionContext executionContext = stepExecution
                .getJobExecution()
                .getExecutionContext();
        this.lines = (List<Item>) executionContext.get("lines");
        logger.info("Lines Processor initialized.");
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        logger.info("Lines Processor ended.");
        return ExitStatus.COMPLETED;
    }
}

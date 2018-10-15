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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSourceUtils;
import org.springframework.stereotype.Component;
import ru.zaets.home.springbatch.demo.onetableprocessing.Item;
import ru.zaets.home.springbatch.demo.test.Status;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class LinesWriter implements Tasklet, StepExecutionListener {

    private final Logger logger = LoggerFactory.getLogger(LinesWriter.class);

    private List<Item> lines;

    @Autowired
    public NamedParameterJdbcTemplate jdbcTemplate;

    @Override
    public void beforeStep(StepExecution stepExecution) {
        ExecutionContext executionContext = stepExecution
                .getJobExecution()
                .getExecutionContext();
        this.lines = (List<Item>) executionContext.get("lines");
        logger.info("Lines Writer initialized.");
    }

    @Override
    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
        for (Item line : lines) {
            logger.info("Wrote line " + line.toString());
        }


        SqlParameterSourceUtils.createBatch(lines);
        final int[] inProgress = jdbcTemplate.batchUpdate("UPDATE onetableprocessing SET processed = true WHERE id = :id",
                SqlParameterSourceUtils.createBatch(lines));

        return RepeatStatus.FINISHED;
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        logger.info("Lines Writer ended.");
        return ExitStatus.COMPLETED;
    }
}

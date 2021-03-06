package ru.zaets.home.springbatch.demo.onetableprocessing.tasklet;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;
import ru.zaets.home.springbatch.demo.onetableprocessing.Item;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Slf4j
@Component
public class LinesReader implements Tasklet, StepExecutionListener {

    private final Logger logger = LoggerFactory.getLogger(LinesReader.class);
    @Autowired
    public NamedParameterJdbcTemplate jdbcTemplate;
    private List<Item> lines;
    int i = 0;

    @Override
    public void beforeStep(StepExecution stepExecution) {
        lines = new ArrayList<>();
        logger.info("Lines Reader initialized.");
    }

    @Override
    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {

        String query = "select * from onetableprocessing where processed is false order by id asc limit 5 offset " + i;
        List<Item> query1 = jdbcTemplate.query(query,
                (rs, rowNum) -> new Item(rs.getLong(1), rs.getBoolean(2)));
        lines.addAll(query1);
        if (query1.size() == 5) {
            i += 5;
            return RepeatStatus.CONTINUABLE;
        } else {
            i = 0;
            return RepeatStatus.FINISHED;
        }
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        stepExecution
                .getJobExecution()
                .getExecutionContext()
                .put("lines", this.lines);
        logger.info("Lines Reader ended.");
        return ExitStatus.COMPLETED;
    }
}

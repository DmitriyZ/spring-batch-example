package ru.zaets.home.springbatch.demo.onetableprocessing;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.sql.PreparedStatement;
import java.sql.SQLException;

@Slf4j
@Component
public class DataProducer {
    @Autowired
    JdbcTemplate jdbcTemplate;

    @Scheduled(fixedRate = 10000, initialDelay = 3005)
    public void generate1() throws SQLException {
        log.info("Generate [OneTableProcessing]!");
        data();
        log.info("End Generate [OneTableProcessing]!");
    }

    private void data() throws SQLException {
        String sql = "INSERT INTO onetableprocessing (processed) VALUES (?)";
        final int batchSize = 50;

        jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {

            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                ps.setBoolean(1, false);
            }

            @Override
            public int getBatchSize() {
                return batchSize;
            }
        });
    }
}

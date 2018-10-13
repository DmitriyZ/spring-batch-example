package ru.zaets.home.springbatch.demo.test;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by dzaets on 15.07.2018.
 * spring-batch-example
 */
@Slf4j
//@Component
public class DataGenerator {


    @Autowired
    JdbcTemplate jdbcTemplate;

    @Scheduled(fixedRate = 10000, initialDelay = 3000)
    public void generate0() throws SQLException {
        log.info("Generate [0]!");
        data();
        log.info("End Generate [0]!");
    }

    @Scheduled(fixedRate = 10000, initialDelay = 3005)
    public void generate1() throws SQLException {
        log.info("Generate [1]!");
        data();
        log.info("End Generate [1]!");
    }

    private void data() throws SQLException {
        String sql = "INSERT INTO numbers (id, value) VALUES (?, ?)";
        final int batchSize = 10;

        jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {

            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                ps.setString(1, UUID.randomUUID().toString());
                ps.setLong(2, ThreadLocalRandom.current().nextInt(1, batchSize + 1));
            }

            @Override
            public int getBatchSize() {
                return batchSize;
            }
        });
    }
}

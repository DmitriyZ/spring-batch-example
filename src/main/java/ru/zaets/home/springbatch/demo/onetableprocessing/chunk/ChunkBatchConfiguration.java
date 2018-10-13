package ru.zaets.home.springbatch.demo.onetableprocessing.chunk;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.support.PostgresPagingQueryProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import ru.zaets.home.springbatch.demo.onetableprocessing.Item;

import javax.sql.DataSource;
import java.util.Collections;

import static org.springframework.batch.item.database.Order.ASCENDING;

@Slf4j
@Configuration
public class ChunkBatchConfiguration {

    @Autowired
    public DataSource dataSource;

    @Autowired
    public NamedParameterJdbcTemplate jdbcTemplate;

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Autowired
    public JobRepository jobRepository;

    @Bean
    public JobLauncher chunkJobLauncher() {
        SimpleJobLauncher simpleJobLauncher = new SimpleJobLauncher();
        simpleJobLauncher.setJobRepository(jobRepository);

        return simpleJobLauncher;
    }

    @Bean
    public Job chunkJob(Step stepChunkProcess) {
        log.info("Create Job Main Job");
        return jobBuilderFactory.get("main_job")
                .incrementer(new RunIdIncrementer())
                .start(stepChunkProcess)
                .build();
    }

    @Bean
    public ItemProcessor<Item, Item> processor() {
        return item -> {
            log.info("Processed: {}", item);
            return item;
        };
    }

    @Bean
    public Step stepChunkProcess(StepBuilderFactory stepBuilderFactory,
                                 ItemReader<Item> reader,
                                 ItemWriter<Item> writer) {

        return stepBuilderFactory.get("step1")
                .<Item, Item>chunk(10)
                .reader(reader)
                .processor(processor())
                .writer(writer)
                .build();
    }

    @Bean
    public ItemReader<Item> reader() {
        JdbcPagingItemReader<Item> reader = new JdbcPagingItemReader<Item>();
        reader.setDataSource(dataSource);
        reader.setRowMapper((rs, i) -> new Item(rs.getLong(1), rs.getBoolean(2)));
        PostgresPagingQueryProvider postgresPagingQueryProvider = new PostgresPagingQueryProvider();

        postgresPagingQueryProvider.setFromClause("onetableprocessing");
        postgresPagingQueryProvider.setSelectClause("*");
        postgresPagingQueryProvider.setSortKeys(Collections.singletonMap("id", ASCENDING));
        postgresPagingQueryProvider.setWhereClause("processed is false");

        reader.setQueryProvider(postgresPagingQueryProvider);
        reader.setPageSize(5);
        return reader;
    }



    @Bean
    public ItemWriter<Item> writer(DataSource dataSource) {
        JdbcBatchItemWriter<Item> writer = new JdbcBatchItemWriterBuilder<Item>()
                .beanMapped()
                .dataSource(dataSource)
                .sql("UPDATE onetableprocessing SET processed = true WHERE id = :id")
                .assertUpdates(true)
                .build();
        return writer;
    }
}

package ru.zaets.home.springbatch.demo.onetableprocessing.aggbatch;

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
import ru.zaets.home.springbatch.demo.onetableprocessing.aggbatch.multiline.AggregateItem;
import ru.zaets.home.springbatch.demo.onetableprocessing.aggbatch.multiline.AggregateItemReader;

import javax.sql.DataSource;
import java.util.Collections;
import java.util.List;

import static org.springframework.batch.item.database.Order.ASCENDING;

@Slf4j
@Configuration
public class SuperBatchConfiguration {

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
    public Job superChunkJob(Step stepSuperChunkProcess) {
        log.info("superChunkJob Job Main Job");
        return jobBuilderFactory.get("super_job")
                .incrementer(new RunIdIncrementer())
                .start(stepSuperChunkProcess)
                .build();
    }

    @Bean
    public ItemProcessor<List<Item>, List<Item>> processorS() {
        return item -> {
            log.info("superChunkJob processed: {}", item);
            return item;
        };
    }

    @Bean
    public Step stepSuperChunkProcess(StepBuilderFactory stepBuilderFactory,
                                 ItemReader<List<Item>> readerS,
                                 ItemProcessor<List<Item>, List<Item>> processorS,
                                 ItemWriter<List<Item>> writerS) {

        return stepBuilderFactory.get("step1")
                .<List<Item>, List<Item>>chunk(10)
                .reader(readerS)
                .processor(processorS)
                .writer(writerS)
                .build();
    }

    @Bean
    public ItemReader<List<Item>> readerS() {
        JdbcPagingItemReader<AggregateItem<Item>> reader = new JdbcPagingItemReader<>();
        reader.setDataSource(dataSource);
        reader.setRowMapper((rs, i) -> new AggregateItem(new Item(rs.getLong(1), rs.getBoolean(2))));
        PostgresPagingQueryProvider postgresPagingQueryProvider = new PostgresPagingQueryProvider();

        postgresPagingQueryProvider.setFromClause("onetableprocessing");
        postgresPagingQueryProvider.setSelectClause("*");
        postgresPagingQueryProvider.setSortKeys(Collections.singletonMap("id", ASCENDING));
        postgresPagingQueryProvider.setWhereClause("processed is false");

        reader.setQueryProvider(postgresPagingQueryProvider);
        reader.setPageSize(5);


        ItemReader<List<Item>> itemReader = new AggregateItemReader<>();
        ((AggregateItemReader<Item>) itemReader).setItemReader(reader);

        return itemReader;
    }



    @Bean
    public ItemWriter<List<Item>> writerS(DataSource dataSource) {
        log.info("Writer: superChunkJob");
        JdbcBatchItemWriter<List<Item>> writer = new JdbcBatchItemWriterBuilder<List<Item>>()
                .beanMapped()
                .dataSource(dataSource)
                .sql("UPDATE onetableprocessing SET processed = true WHERE id = :id")
                .assertUpdates(true)
                .build();
        return writer;
    }
}

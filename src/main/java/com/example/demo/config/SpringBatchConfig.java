package com.example.demo.config;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import com.example.demo.model.Customer;
import com.example.demo.partition.ColumnRangePartitioner;
import com.example.demo.repository.CustomerRepository;

import lombok.AllArgsConstructor;


@Configuration // Marks this class as Spring configuration
@EnableBatchProcessing // Enables Spring Batch features (creates JobRepository, JobLauncher, etc.)
@AllArgsConstructor // Lombok annotation for constructor injection
public class SpringBatchConfig {

    // Factory used to build Jobs
    private JobBuilderFactory jobBuilderFactory;

    // Factory used to build Steps
    private StepBuilderFactory stepBuilderFactory;

    // JPA repository to save Customer data
    private CustomerRepository customerRepository;
    
    // Custom writer implementation (used in partitioned step)
    private CustomerWriter customerWriter;

    // ===================== READER =====================

    @Bean
    public FlatFileItemReader<Customer> reader() {

        // Reader to read data from a CSV file
        FlatFileItemReader<Customer> itemReader = new FlatFileItemReader<>();

        // File location
        itemReader.setResource(new FileSystemResource("src/main/resources/customers.csv"));

        // Name of the reader
        itemReader.setName("csvReader");

        // Skip header row (first line)
        itemReader.setLinesToSkip(1);

        // Set line mapper (maps CSV row → Customer object)
        itemReader.setLineMapper(lineMapper());

        return itemReader;
    }

    // Maps CSV columns to Customer fields
    private LineMapper<Customer> lineMapper() {

        DefaultLineMapper<Customer> lineMapper = new DefaultLineMapper<>();

        // Tokenizer splits CSV line based on comma
        DelimitedLineTokenizer lineTokenizer = new DelimitedLineTokenizer();
        lineTokenizer.setDelimiter(",");
        lineTokenizer.setStrict(false);

        // These column names must match Customer class fields
        lineTokenizer.setNames(
            "id", "firstName", "lastName", "email",
            "gender", "contactNo", "country", "dob"
        );

        // Maps tokenized values to Customer object
        BeanWrapperFieldSetMapper<Customer> fieldSetMapper =
                new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(Customer.class);

        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);

        return lineMapper;
    }

    // ===================== PROCESSOR =====================

    @Bean
    public CustomerProcessor processor() {
        // Used for validation / transformation logic
        return new CustomerProcessor();
    }

    // ===================== WRITER =====================

    @Bean
    public RepositoryItemWriter<Customer> writer() {

        // Writer that uses Spring Data JPA repository
        RepositoryItemWriter<Customer> writer =
                new RepositoryItemWriter<>();

        // Inject repository
        writer.setRepository(customerRepository);

        // Calls save() method for each item
        writer.setMethodName("save");

        return writer;
    }

    // ===================== SIMPLE STEP (Non-Partitioned) =====================

    @Bean
    public Step step1() {

        return stepBuilderFactory.get("csv-step")

                // Chunk size = 10 (10 records per transaction)
                .<Customer, Customer>chunk(10)

                .reader(reader())
                .processor(processor())
                .writer(writer())

                // Enables multithreading
                .taskExecutor(taskExecutor())

                .build();
    }

    // ===================== JOB =====================

    @Bean
    public Job runJob() {

        // Job starts with masterStep (partitioned step)
        return jobBuilderFactory.get("importCustomers")
                .flow(masterStep())
                .end()
                .build();
    }

    // ===================== PARTITIONING =====================

    // Custom partitioner (splits workload into partitions)
    @Bean
    public ColumnRangePartitioner partitioner() {
        return new ColumnRangePartitioner();
    }

    // Handles execution of partitions using multiple threads
    @Bean
    public PartitionHandler partitionHandler() {

        TaskExecutorPartitionHandler taskExecutorPartitionHandler =
                new TaskExecutorPartitionHandler();

        // Number of parallel partitions
        taskExecutorPartitionHandler.setGridSize(4);

        // Thread pool executor
        taskExecutorPartitionHandler.setTaskExecutor(taskExecutor());

        // Each partition runs slaveStep
        taskExecutorPartitionHandler.setStep(slaveStep());

        return taskExecutorPartitionHandler;
    }

    // ===================== SLAVE STEP =====================

    @Bean
    public Step slaveStep() {

        return stepBuilderFactory.get("slaveStep")

                // Each chunk processes 250 records per transaction
                .<Customer, Customer>chunk(250)

                .reader(reader())
                .processor(processor())

                // Using custom writer
                .writer(customerWriter)

                .build();
    }

    // ===================== MASTER STEP =====================

    @Bean
    public Step masterStep() {

        return stepBuilderFactory.get("masterStep")

                // Partitioning slaveStep into multiple partitions
                .partitioner(slaveStep().getName(), partitioner())

                // Handles parallel execution
                .partitionHandler(partitionHandler())

                .build();
    }

    // ===================== THREAD POOL =====================

    @Bean
    public TaskExecutor taskExecutor() {

        ThreadPoolTaskExecutor taskExecutor =
                new ThreadPoolTaskExecutor();

        // 4 threads minimum
        taskExecutor.setCorePoolSize(4);

        // Max 4 threads
        taskExecutor.setMaxPoolSize(4);

        // Queue capacity
        taskExecutor.setQueueCapacity(4);

        return taskExecutor;
    }
}


}

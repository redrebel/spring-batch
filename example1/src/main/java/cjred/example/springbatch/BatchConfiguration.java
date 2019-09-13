package cjred.example.springbatch;

import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.json.builder.JsonItemReaderBuilder;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableMBeanExport;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;

import java.io.File;
import java.net.MalformedURLException;
import java.sql.SQLOutput;
import java.util.Date;

@EnableBatchProcessing
@Configuration
public class BatchConfiguration {
    @Autowired
    JobBuilderFactory jobBuilderFactory;

    @Autowired
    StepBuilderFactory stepBuilderFactory;

    @Bean
    public JsonItemReader jsonItemReader() throws MalformedURLException {
        return new JsonItemReaderBuilder()
                .jsonObjectReader(new JacksonJsonObjectReader(Movie.class))
                .resource(new UrlResource(
                        "https://raw.githubusercontent.com/prust/wikipedia-movie-data/master/movies.json"))
                .name("movieJsonItemReader")
                .build();
    }

    @Bean
    public ItemProcessor<Movie, MovieGenre> movieListItemProcessor(){
        return movie -> new MovieGenre(movie.getTitle(), movie.getGenres().toString());
    }

    @Bean
    public FlatFileItemWriter movieGenreWriter(){
        return new FlatFileItemWriterBuilder()
                .name("movieGenreWriter")
                .resource(new FileSystemResource("out/movies.csv"))
                .delimited()
                .delimiter(",")
                .names(new String[] {"title", "genre"})
                .build();
    }

    @Bean
    public Step movieStep(StepExecutionListener movieStepLidtener) throws MalformedURLException {
        return stepBuilderFactory
                .get("movieStep")
                .listener(movieStepLidtener)
                .<Movie, MovieGenre>chunk(10)
                .reader(jsonItemReader())
                .processor(movieListItemProcessor())
                .writer(movieGenreWriter())
                .build();
    }
    @Bean
    public Job movieJob( Step movieStep, Step listStep){
        return jobBuilderFactory.get("movieJob")
                .incrementer(new RunIdIncrementer())
//                .listener(movieStepLidtener)
                .flow(movieStep)
                .next(listStep)
                .end()
                .build();
    }

    // tag::Listener
    @Bean
    public StepExecutionListener movieStepListener() {
        return new StepExecutionListener() {
            @Override
            public void beforeStep(StepExecution stepExecution) {
                stepExecution.getExecutionContext().put("start",
                        new Date().getTime());
                System.out.println("Step name:" + stepExecution.getStepName()
                + "Started");
            }

            @Override
            public ExitStatus afterStep(StepExecution stepExecution) {
                long elapsed = new Date().getTime()
                        - stepExecution.getExecutionContext().getLong("start");
                System.out.println("Step name:" + stepExecution.getStepName()
                + "Ended. Running time is " + elapsed + " miliseconds.");
                System.out.println("Read Count:" + stepExecution.getReadCount()
                + " Write Count:" + stepExecution.getWriteCount());
                return ExitStatus.COMPLETED;
            }
        };
    }

    // tag::Tasklet
    @Bean
    public Step listStep() {
        return stepBuilderFactory.get("listStep")
                .tasklet((stepContribution, chunkContext) -> {
                  Resource directory = new FileSystemResource("out");
                  System.out.println(directory.getFile()
                  + " directory is available");
                  for(File file : directory.getFile().listFiles()) {
                      System.out.println(file.getName()
                      + " is available");
                  }
                  return RepeatStatus.FINISHED;
                }).build();
    }
}

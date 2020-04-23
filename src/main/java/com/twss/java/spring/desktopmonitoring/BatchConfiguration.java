package com.twss.java.spring.desktopmonitoring;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.twss.java.spring.desktopmonitoring.model.IdealReport;
import com.twss.java.spring.desktopmonitoring.model.UserProcessInfo;
import com.twss.java.spring.desktopmonitoring.utilitybeans.TimeFormatter;

// tag::setup[]
@Configuration
@EnableBatchProcessing
public class BatchConfiguration {
	
	final static Logger logger = LoggerFactory.getLogger(BatchConfiguration.class);


	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;
	
	@Autowired
	public JdbcTemplate jdbcTemplate;
	
	
	private SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
	
	private static final String DATE_FORMAT="dd/MM/yyyy";
	private TimeFormatter dateTime = new TimeFormatter();

	
	
	 @Autowired
	 private Job job;
		 
	@Bean(destroyMethod="")
	public ItemReader<UserProcessInfo> reader(DataSource dataSource) {
		JdbcCursorItemReader<UserProcessInfo> databaseReader = new JdbcCursorItemReader<UserProcessInfo>();
		 databaseReader.setDataSource(dataSource);
		 java.sql.Date date = dateTime.getCurrentSQLDate();
		 SimpleDateFormat dateformatJava = new SimpleDateFormat(DATE_FORMAT);
			String today = dateformatJava.format(date);
			System.out.println("Today's date is ==="+today);
		 String sql = "select distinct username as userName,"
		 		+ " IFNULL((select `value` from desktop_monitoring_config"
		 		+ " where active='Y'"
		 		+ " and `key`='config_date'),'"+today+"') as `date`"
		 		+ " from screenshot"
		 		+ " where `date` = IFNULL((select `value` from desktop_monitoring_config "
		 		+ " where active='Y'"
		 		+ " and `key`='config_date'),'"+today+"')"
		 		+ " and gcs_image_path is not null"
		 		+ " and idleStatus ='false'";
		 
	        System.out.println(sql);
	        logger.info("reader called" + sql);
	        databaseReader.setSql(sql);
	        databaseReader.setRowMapper(new BeanPropertyRowMapper<>(UserProcessInfo.class));
	 
	        return databaseReader;
		
	}
	 
		 
	 @Bean
	public ItemProcessor<UserProcessInfo, IdealReport> processor() {
		return new DesktopMonitoringProcessor();
	}

		
	@Bean
	public ItemWriter<IdealReport> writer() throws IOException {
		return new DesktopMonitoringWriter();
	}
	
	
	@Bean
	public Job importUserJob(JobCompletionNotificationListener listener, Step step1) {
		return jobBuilderFactory.get("importUserJob")
			.incrementer(new RunIdIncrementer())
			.listener(listener)
			.flow(step1)
			.end()
			.build();
	}

	@Bean
	public Step step1(ItemReader<UserProcessInfo> reader) throws IOException {
		return stepBuilderFactory.get("step1")
			.<UserProcessInfo, IdealReport> chunk(1)
			.reader(reader).processor(processor())
			.writer(writer())
			.build();
	}
	
		    
}

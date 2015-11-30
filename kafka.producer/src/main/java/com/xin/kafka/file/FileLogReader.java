package com.xin.kafka.file;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.xin.kafka.producer.Producer;
import com.xin.kafka.producer.ProducerFactory;
import com.xin.kafka.util.Resources;

public class FileLogReader extends LogReader implements Runnable {
  
  private final Path logFilePath;
  
  private final int persistLines;
  private final long persistPeriod;
  private final long readInterval;
  private final Pattern timestampPattern;
  private final int retryCount;
  private final Path checkPointPath;
  private final String topic;
  
  private boolean finished = false;
  private long startTime;
  private int linesNotPersist = 0;
  
  public FileLogReader(Configuration conf, Path logFilePath) {
    super(conf);
    this.logFilePath = logFilePath;
    this.persistLines = conf.getPersistLines();
    this.persistPeriod = conf.getPersistPeriod();
    this.readInterval = conf.getReadInterval();
    this.timestampPattern = conf.getTimestampPattern();
    this.retryCount = conf.getRetryCount();
    this.checkPointPath = Paths.get(conf.getCheckPointPath().toString(), logFilePath.getFileName().toString());
    try {
      this.topic = Resources.getResourceAsProperties("topic.properties").getProperty("topics");
    } catch (IOException e) {
      log.error("[FileLogReader]Cannot get topic, error : {}", e);
      throw new RuntimeException("[FileLogReader]Cannot get topic.", e);
    }
  }

  @Override
  public void run() {
    
    setStartTime();
    start = CheckPoint.get(checkPointPath, logFilePath);
    prepare(logFilePath);
    while (!finished) {
      try {
        List<String> lines = readLines();
        if (lines == null || lines.isEmpty()) {
          if (isHistoryLog()) {
            finished();
            return;
          }
        }
        // send to kafka
        Producer producer = ProducerFactory.getProducer(topic);
        int currentRetry = 0;
        while (currentRetry <= retryCount) {
          if (!producer.send(lines)) {
            currentRetry++;
            sleep(readInterval);
          } else {
            break;
          }
        }
        if (currentRetry > retryCount) {
          log.warn("[FileLogReader]Cannot send log to kafka cluster, retry count : {}", currentRetry);
          return;
        }
        for (String line : lines) {
          log.info("[FileLogReader]FileLogReader read : {}", line);
        }
        linesNotPersist += lines.size();
        if (need2Persist()) {
          CheckPoint.persist(checkPointPath, logFilePath, start);
          resetReader();
        }
        sleep(readInterval);
      } catch (IOException e) {
        if (!isHistoryLog()) {
          CheckPoint.save2TmpFile(checkPointPath);
        }
      } 
    }
    CheckPoint.persist(checkPointPath, logFilePath, start);
  }
  
  public void finished() {
    finished = true;
  }
  
  public boolean isFinish() {
    return finished;
  }
  
  private void setStartTime() {
    startTime = System.currentTimeMillis();
  }
  
  private boolean need2Persist() {
    long currentTime = System.currentTimeMillis();
    if (linesNotPersist >= persistLines || (currentTime - startTime) >= persistPeriod) {
      return true;
    }
    return false;
  }
  
  public void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      
    } 
  }
  
  private boolean isHistoryLog() {
    Matcher matcher = timestampPattern.matcher(logFilePath.toString()); 
    while (matcher.find()) {
      return true;
    }
    return false;
  }
  
  public void restart() {
    finished = false;
    resetReader();
  }
  
  private void resetReader() {
    linesNotPersist = 0;
    startTime = System.currentTimeMillis();
  }
}

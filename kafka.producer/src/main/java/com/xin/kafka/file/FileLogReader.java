package com.xin.kafka.file;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.xin.kafka.file.ObserverTest.Producer;
import com.xin.kafka.util.FileUtil;

public class FileLogReader implements Runnable {
  
  private boolean finished = false;
  
  private Producer producer;
  private String path;
  
  private final int maxLine;
  private final int persistLines;
  private final long persistPeriod;
  private final long readInterval;
  private final Pattern timestampPattern;
  
  private long startTime;
  private int linesNotPersist = 0;
  private String checkPointPath;
  
  public FileLogReader(Configuration conf, String path) {
    this.path = path;
    this.maxLine = conf.getMaxLine();
    this.persistLines = conf.getPersistLines();
    this.persistPeriod = conf.getPersistPeriod();
    this.readInterval = conf.getRetryInterval();
    this.timestampPattern = conf.getTimestampPattern();
    this.checkPointPath = conf.getCheckPointPath() + "/" + Paths.get(path).getFileName();
  }

  @Override
  public void run() {
    
    setStartTime();
    int startLine = CheckPoint.get(checkPointPath);
    while (!finished) {
      try {
        final int from = startLine;
        List<String> lines = FileUtil.read(path, from, maxLine);
        if (lines == null || lines.isEmpty()) {
          if (isHistoryLog()) {
            finished();
            return;
          }
        }
        // send to kafka
        for (String line : lines) {
          System.out.println("fileLogReader read : " + line);
        }
        startLine += lines.size();
        linesNotPersist += lines.size();
        if (need2Persist()) {
          CheckPoint.persist(checkPointPath, startLine);
          resetReader();
        }
        sleep(readInterval);
      } catch (IOException e) {
        if (!isHistoryLog()) {
          CheckPoint.save2TmpFile(checkPointPath);
        }
      } 
    }
    CheckPoint.persist(checkPointPath, startLine);
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
    Matcher matcher = timestampPattern.matcher(path); 
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

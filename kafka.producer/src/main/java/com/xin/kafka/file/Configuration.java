package com.xin.kafka.file;

import java.util.regex.Pattern;

public class Configuration {

  // 一次最多读取的行数
  private int maxLine = 50;
  // 达到多少行时需要持久化
  private int persistLines = 100;
  // 持久化最大时间间隔ms
  private long persistPeriod = 500;
  // 失败重试次数
  private int retryCount = 3;
  // 失败重试间隔时间
  private long retryInterval = 100;
  // 检查点文件保存路径
  private String checkPointPath;
  private long monitorInterval = 1000;
  
  private Pattern timestampPattern; 
  
  public int getMaxLine() {
    return maxLine;
  }

  public int getPersistLines() {
    return persistLines;
  }

  public long getPersistPeriod() {
    return persistPeriod;
  }

  public int getRetryCount() {
    return retryCount;
  }

  public long getRetryInterval() {
    return retryInterval;
  }

  public String getCheckPointPath() {
    return checkPointPath;
  }

  public long getMonitorInterval() {
    return monitorInterval;
  }

  public Pattern getTimestampPattern() {
    return timestampPattern;
  }

  public static class Builder {
    private Configuration conf = new Configuration();
    
    public Builder(String checkPointPath) {
      conf.checkPointPath = checkPointPath;
    }
    
    public Builder maxLine(int maxLine) {
      conf.maxLine = maxLine;
      return this;
    }
    
    public Builder persistLines(int persistLines) {
      conf.persistLines = persistLines;
      return this;
    }
    
    public Builder persistPeriod(long persistPeriod) {
      conf.persistPeriod = persistPeriod;
      return this;
    }
    
    public Builder retryCount(int retryCount) {
      conf.retryCount = retryCount;
      return this;
    }
    
    public Builder retryInterval(long retryInterval) {
      conf.retryInterval = retryInterval;
      return this;
    }
    
    public Builder monitorInterval(long monitorInterval) {
      conf.monitorInterval = monitorInterval;
      return this;
    }
    
    public Builder suffixTimestampRegex(String suffixTimestampRegex) {
      conf.timestampPattern = Pattern.compile(suffixTimestampRegex);
      return this;
    }
    
    public Configuration build() {
      return conf;
    }
  }
}

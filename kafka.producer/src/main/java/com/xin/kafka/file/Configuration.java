package com.xin.kafka.file;

import java.nio.file.Path;
import java.util.regex.Pattern;

public class Configuration {

  // 达到多少行时需要持久化
  private int persistLines = 100;
  // 持久化最大时间间隔ms
  private long persistPeriod = 500;
  // 失败重试次数
  private int retryCount = 3;
  // 失败重试间隔时间ms
  private long retryInterval = 100;
  // reader线程读取间隔时间ms
  private long readInterval = 100;
  // bytebuffer缓存大小
  private int maxSize = 1000;
  // 被监控的log文件路径
  private Path logFilePath;
  // 检查点文件保存路径
  private Path checkPointPath;
  // monitor线程检查间隔时间ms
  private long monitorInterval = 1000;
  // 切分后的点击log的时间戳后缀, 默认yyMMddHHmmss
  private Pattern timestampPattern = Pattern.compile("[0-9]{12}$"); 
  
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

  public long getReadInterval() {
    return readInterval;
  }

  public int getMaxSize() {
    return maxSize;
  }

  public Path getCheckPointPath() {
    return checkPointPath;
  }

  public long getMonitorInterval() {
    return monitorInterval;
  }

  public Pattern getTimestampPattern() {
    return timestampPattern;
  }

  public Path getLogFilePath() {
    return logFilePath;
  }

  public static class Builder {
    private Configuration conf = new Configuration();
    
    public Builder(Path logFilePath, Path checkPointPath) {
      conf.logFilePath = logFilePath;
      conf.checkPointPath = checkPointPath;
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
    
    public Builder readInterval(long readInterval) {
      conf.readInterval = readInterval;
      return this;
    }
    
    public Builder maxSize(int maxSize) {
      conf.maxSize = maxSize;
      return this;
    }
    
    public Configuration build() {
      return conf;
    }
  }
}

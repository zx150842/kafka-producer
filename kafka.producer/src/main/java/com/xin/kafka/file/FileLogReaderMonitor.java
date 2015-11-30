package com.xin.kafka.file;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.xin.kafka.thread.SimpleThreadFactory;
import com.xin.kafka.util.FileUtil;
import com.xin.kafka.util.Pair;

public class FileLogReaderMonitor {

  static final Logger log = LoggerFactory.getLogger(FileLogReaderMonitor.class);
  
  private Map<Path, FileLogReader> readerMap = Maps.newConcurrentMap();
  private Map<FileLogReader, Thread> threadMap = Maps.newConcurrentMap();
  
  private final SimpleThreadFactory threadFactory;
  private Configuration conf;
  private final Monitor monitor;
  
  public FileLogReaderMonitor(Configuration conf) {
    this.conf = conf;
    threadFactory = new SimpleThreadFactory();
    monitor = new Monitor(conf.getMonitorInterval());
  }
  
  public void start() {
    List<Path> logFilePaths = getUnFinishLogFilePath();
    for (Path logFilePath : logFilePaths) {
      FileLogReader reader = new FileLogReader(conf, logFilePath);
      Thread thread = threadFactory.newThread(reader);
      threadMap.put(reader, thread);
      readerMap.put(logFilePath, reader);
      thread.start();
    }
    Thread monitorThread = threadFactory.newThread(monitor);
    monitorThread.start();
  }
  
  public void add(Path path) {
    FileLogReader reader;
    Thread thread;
    if (readerMap.containsKey(path)) {
      reader = readerMap.get(path);
      thread = threadMap.get(reader);
      if (thread != null && thread.isAlive()) {
        reader.finished();
        while (thread.isAlive());
      } 
      reader.restart();
    } else {
      reader = new FileLogReader(conf, path);
      readerMap.put(path, reader);
    }
    prepareCheckPoint(Paths.get(conf.getCheckPointPath().toString(), path.getFileName().toString()));
    thread = threadFactory.newThread(reader);
    threadMap.put(reader, thread);
    thread.start();
  }
  
  public void remove(Path path) {
    if (readerMap.containsKey(path)) {
      FileLogReader reader = readerMap.get(path);
      reader.finished();
      Thread thread = threadMap.get(reader);
      if (thread != null) {
        while (thread.isAlive());
        threadMap.remove(reader);
      }
    }
  }
  
  public Pair<FileLogReader, Thread> getFileLogReader(String path) {
    if (readerMap.containsKey(path)) {
      FileLogReader reader = readerMap.get(path);
      Thread thread = threadMap.get(reader);
      return new Pair<FileLogReader, Thread>(reader, thread);
    }
    return null;
  }
  
  private List<Path> getUnFinishLogFilePath() {
    List<Path> logFilePathes = Lists.newArrayList();
    try {
      List<Path> checkPointPaths = FileUtil.getChild(conf.getCheckPointPath(), false);
      for (Path checkPointPath : checkPointPaths) {
          List<String> lines = FileUtil.readLines(checkPointPath);
          logFilePathes.add(Paths.get(lines.get(0)));
      }
    } catch (IOException e) {
      log.error("[Monitor]Cannot load checkpoint file, error : {}. ", e);
    }
    return logFilePathes;
  }
  
  public void finishMonitor() {
    monitor.finished();
  }
  
  private void prepareCheckPoint(Path newCheckPointPath) {
    log.info("[Monitor]Add checkPoint : " + newCheckPointPath);
    // 删除与将要监控的path重名的检查点
    CheckPoint.remove(newCheckPointPath);
    // 将tmp checkpoint赋给现在的checkpoint
    try {
      List<Path> checkcPointPaths = FileUtil.getChild(conf.getCheckPointPath(), false);
      for (Path checkPointPath : checkcPointPaths) {
        String fileName = checkPointPath.getFileName().toString();
        if (fileName.endsWith("tmp")) {
          long tmpFileTimestamp = checkPointPath.toFile().lastModified();
          long currentTimestamp = System.currentTimeMillis();
          if (Math.abs(tmpFileTimestamp - currentTimestamp) < 1000) {
            CheckPoint.rename(checkPointPath, newCheckPointPath);
            break;
          }
        }
      }
    } catch (IOException e) {
      log.error("[Monitor]Cannot get checkpoint path, error : {}", e);
    }
  }
  
  private class Monitor implements Runnable {
    
    private boolean finished = false;
    private long monitorInterval;
    
    public Monitor(long monitorInterval) {
      this.monitorInterval = monitorInterval;
    }
    
    public void finished() {
      finished = true;
    }
    
    @Override
    public void run() {
      
      while (!finished) {
        for (Map.Entry<Path, FileLogReader> entry : readerMap.entrySet()) {
          Path path = entry.getKey();
          FileLogReader reader = entry.getValue();
          Thread thread = threadMap.get(reader);
          if (!reader.isFinish()) {
            if (thread == null || !thread.isAlive()) {
              thread = threadFactory.newThread(reader);
              threadMap.put(reader, thread);
              thread.start();
            }
          } else {
            while (thread != null && thread.isAlive());
            readerMap.remove(path);
            threadMap.remove(reader);
          }
        }
        sleep(monitorInterval);
      }
    }
    
    private void sleep(long millis) {
      try {
        Thread.sleep(millis);
      } catch (InterruptedException e) {
        log.error("[Monitor]Monitor was interrupted, now exit monit, error : {} ");
        finished();
      } 
    }
  }
}

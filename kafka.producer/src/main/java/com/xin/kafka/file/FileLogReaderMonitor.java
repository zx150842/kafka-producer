package com.xin.kafka.file;

import java.io.File;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.xin.kafka.thread.SimpleThreadFactory;
import com.xin.kafka.util.FileUtil;
import com.xin.kafka.util.Pair;

public class FileLogReaderMonitor {

  private Map<String, FileLogReader> readerMap = Maps.newConcurrentMap();
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
    List<String> pathes = getUnproducePath();
    for (String path : pathes) {
      FileLogReader reader = new FileLogReader(conf, path);
      Thread thread = threadFactory.newThread(reader);
      threadMap.put(reader, thread);
      readerMap.put(path, reader);
      thread.start();
    }
    Thread monitorThread = threadFactory.newThread(monitor);
    monitorThread.start();
  }
  
  public void add(String path) {
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
    // 删除旧的检查点
    String checkPointPath = conf.getCheckPointPath() + "/" + Paths.get(path).getFileName();
    System.out.println("---monitor add---");
    System.out.println("Add checkPoint : " + checkPointPath);
    System.out.println("Create path : " + path);
    CheckPoint.remove(checkPointPath);
    findCheckPoint(checkPointPath);
    thread = threadFactory.newThread(reader);
    threadMap.put(reader, thread);
    thread.start();
  }
  
  public void remove(String path) {
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
  
  private List<String> getUnproducePath() {
    File checkPointDir = new File(conf.getCheckPointPath());
    List<File> files = FileUtil.getChild(checkPointDir, false);
    List<String> pathes = Lists.newArrayList();
    for (File file : files) {
      pathes.add(file.getAbsolutePath());
    }
    return pathes;
  }
  
  public void finishMonitor() {
    monitor.finished();
  }
  
  private void findCheckPoint(String path) {
    List<String> pathes = getUnproducePath();
    for (String checkPointPath : pathes) {
      File file = new File(checkPointPath);
      String fileName = file.getName();
      if (fileName.endsWith("tmp")) {
        long tmpFileTimestamp = file.lastModified();
        long currentTimestamp = System.currentTimeMillis();
        if (Math.abs(tmpFileTimestamp - currentTimestamp) < 1000) {
          CheckPoint.rename(checkPointPath, path);
          break;
        }
      }
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
        for (Map.Entry<String, FileLogReader> entry : readerMap.entrySet()) {
          String path = entry.getKey();
          FileLogReader reader = entry.getValue();
          Thread thread = threadMap.get(reader);
          if (!reader.isFinish()) {
            if (!thread.isAlive()) {
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
        finished();
      } 
    }
  }
}

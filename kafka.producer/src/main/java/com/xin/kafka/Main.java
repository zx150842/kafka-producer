package com.xin.kafka;

import java.nio.file.Paths;

import com.xin.kafka.file.Configuration;
import com.xin.kafka.file.FileLogReaderMonitor;
import com.xin.kafka.file.FileLogReaderObserver;
import com.xin.kafka.file.FileWatcher;

public class Main {

  public static void main(String[] args) throws Exception {
    Main main = new Main();
    String logFilePath = "E:\\test";
    String checkPointPath = "E:\\checkpoint";
    Configuration conf = new Configuration.Builder(checkPointPath).build();
    main.execute(conf, logFilePath, checkPointPath);
  }
  
  public void execute(Configuration conf, String logFilePath, String checkPointFilePath) {
    FileLogReaderMonitor monitor = new FileLogReaderMonitor(conf);
    FileLogReaderObserver observer = new FileLogReaderObserver(monitor);
    FileWatcher watcher = new FileWatcher(Paths.get(logFilePath), false);
    watcher.addObserver(observer);
    monitor.start();
    watcher.execute();
  }
}

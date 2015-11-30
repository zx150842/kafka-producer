package com.xin.kafka;

import java.nio.file.Path;
import java.nio.file.Paths;

import com.xin.kafka.file.Configuration;
import com.xin.kafka.file.FileLogReaderMonitor;
import com.xin.kafka.file.FileLogReaderObserver;
import com.xin.kafka.file.FileWatcher;

public class Main {

  public static void main(String[] args) throws Exception {
    Main main = new Main();
    Path logFilePath = Paths.get("E:\\test");
    Path checkPointPath = Paths.get("E:\\checkpoint");
    Configuration conf = new Configuration.Builder(logFilePath, checkPointPath).build();
    main.execute(conf);
  }
  
  public void execute(Configuration conf) {
    FileLogReaderMonitor monitor = new FileLogReaderMonitor(conf);
    FileLogReaderObserver observer = new FileLogReaderObserver(monitor);
    FileWatcher watcher = new FileWatcher(conf.getLogFilePath(), true);
    watcher.addObserver(observer);
    monitor.start();
    watcher.execute();
  }
}

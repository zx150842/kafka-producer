package com.xin.kafka.file;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;

import java.nio.file.WatchEvent.Kind;
import java.util.Observable;
import java.util.Observer;

public class FileLogReaderObserver implements Observer {

  private FileLogReaderMonitor monitor;
  
  public FileLogReaderObserver(FileLogReaderMonitor monitor) {
    this.monitor = monitor;
  }
  
  @Override
  public void update(Observable o, Object arg) {
    FileWatcher.Info info = (FileWatcher.Info) arg;
    if (info != null) {
      Kind<?> kind = info.getKind();
      String filePath = info.getPath();
      System.out.println("filelogreader observer get " + kind + ", " + filePath);
      if (ENTRY_MODIFY.equals(kind)) {
        // do nothing
      } else if (ENTRY_DELETE.equals(kind)) {
        monitor.remove(filePath);
      } else if (ENTRY_CREATE.equals(kind)) {
        monitor.add(filePath);
      } 
    }
  }
  
}

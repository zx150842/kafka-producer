package com.xin.kafka.file;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;

import java.nio.file.Path;
import java.nio.file.WatchEvent.Kind;
import java.util.Observable;
import java.util.Observer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileLogReaderObserver implements Observer {

  static final Logger log = LoggerFactory.getLogger(FileLogReaderObserver.class);
  
  private FileLogReaderMonitor monitor;
  
  public FileLogReaderObserver(FileLogReaderMonitor monitor) {
    this.monitor = monitor;
  }
  
  @Override
  public void update(Observable o, Object arg) {
    FileWatcher.Info info = (FileWatcher.Info) arg;
    if (info != null) {
      Kind<?> kind = info.getKind();
      Path path = info.getPath();
      log.info("[Observer]Filelogreader observer receive kind : {}, path : {}. ", kind, path);
      if (ENTRY_MODIFY.equals(kind)) {
        // do nothing
      } else if (ENTRY_DELETE.equals(kind)) {
        monitor.remove(path);
      } else if (ENTRY_CREATE.equals(kind)) {
        monitor.add(path);
      } 
    }
  }
  
}

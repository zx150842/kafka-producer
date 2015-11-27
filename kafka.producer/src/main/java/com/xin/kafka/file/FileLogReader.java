package com.xin.kafka.file;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;

import java.nio.file.WatchEvent;
import java.util.Observable;
import java.util.Observer;

import com.xin.kafka.file.ObserverTest.Producer;

public class FileLogReader {
  
  private Producer producer;

  public static class FileLogReaderObserver implements Observer {

    @Override
    public void update(Observable o, Object arg) {
      FileWatcher.Info info = (FileWatcher.Info) arg;
      if (info != null) {
        WatchEvent<?> event = info.getEvent();
        if (ENTRY_MODIFY.equals(event)) {
          
        } else if (ENTRY_MODIFY.equals(event)) {
          
        } else if (ENTRY_CREATE.equals(event)) {
          
        } else {
          
        }
      }
    }
  }
}

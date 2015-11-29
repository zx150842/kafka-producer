package com.xin.kafka.file;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Observable;
import java.util.Observer;

import org.junit.Test;

import static java.nio.file.StandardWatchEventKinds.*;

public class FileWatchTest {

  @Test
  public void test() {
    KafkaProducer producer = new KafkaProducer();
    DirectoryWatcher watcher = new DirectoryWatcher("D:\\360Downloads");
    watcher.addObserver(producer);
    watcher.watch();
  }
  
  public class FileSystemArgs {
    String fileName;
    WatchEvent.Kind<?> kind;
    
    public FileSystemArgs(String fileName, WatchEvent.Kind<?> kind) {
      this.fileName = fileName;
      this.kind = kind;
    }

    public String getFileName() {
      return fileName;
    }

    public WatchEvent.Kind<?> getKind() {
      return kind;
    }
  }
  
  public class DirectoryWatcher extends Observable {
    
    private WatchService watcher;
    
    public DirectoryWatcher(String dir) {
      FileSystem fileSystem = FileSystems.getDefault();
      Path path = fileSystem.getPath(dir);
      try {
        WatchService watcher = fileSystem.newWatchService();
        WatchKey key = path.register(watcher, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);
        this.watcher = watcher;
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    
    public void watch() {
      while (true) {
        WatchKey key;
        try {
          Thread.sleep(10000);
          key = watcher.take();
        } catch (InterruptedException e) {
          return;
        }
        for (WatchEvent<?> event : key.pollEvents()) {
          if (event.kind() == OVERFLOW) {
            continue;
          }
          Path path = (Path)event.context();
          notice(path.getFileName().toString(), event.kind());
        }
        key.reset();
      }
    }
    
    public void notice(String fileName, WatchEvent.Kind<?> kind) {
      setChanged();
      notifyObservers(new FileSystemArgs(fileName, kind));
    }
  }
  
  public class KafkaProducer implements Observer {

    public void update(Observable o, Object arg) {
      FileSystemArgs args = (FileSystemArgs) arg;
      System.out.println(args.getFileName() + " " + args.getKind());
    }
  }
  
}

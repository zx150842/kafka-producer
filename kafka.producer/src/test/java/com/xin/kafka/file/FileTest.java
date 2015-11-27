package com.xin.kafka.file;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;

import org.junit.Test;

public class FileTest {

  @Test
  public void fileWatchTest() throws IOException {
    FileSystem fileSystem = FileSystems.getDefault();
    WatchService watcher = fileSystem.newWatchService();
    Path path = fileSystem.getPath("D:\\");
    WatchKey key = path.register(watcher, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_DELETE, StandardWatchEventKinds.ENTRY_MODIFY);
    while (true) {
      for (WatchEvent<?> event : key.pollEvents()) {
        System.out.println("dir changed");
      }
    }
  }
  
}

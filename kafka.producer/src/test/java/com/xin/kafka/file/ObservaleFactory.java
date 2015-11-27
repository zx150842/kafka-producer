package com.xin.kafka.file;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.Map;
import java.util.Observable;

import static java.nio.file.StandardWatchEventKinds.*;

public class ObservaleFactory {

  private final WatchService watcher;
  private final Map<WatchKey, Path> diretoriesByKey = new HashMap<WatchKey, Path>();
  private final Path directory;
  private final boolean recursive;
  
  private ObservaleFactory(final Path path, final boolean recursive) throws IOException {
    final FileSystem fileSystem = path.getFileSystem();
    watcher = fileSystem.newWatchService();
    directory = path;
    this.recursive = recursive;
  }
  
  private Observable create() {
    return new Observable();
  }
  
  private void registerAll(final Path rootDirectory) throws IOException {
    Files.walkFileTree(rootDirectory, new SimpleFileVisitor<Path>(){
      @Override
      public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
        register(dir);
        return FileVisitResult.CONTINUE;
      }
    });
  }
  
  private void register(final Path dir) throws IOException {
    final WatchKey key = dir.register(watcher, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);
    diretoriesByKey.put(key, dir);
  }
  
  private void registerNewDirectory() {
    
  }
}

package com.xin.kafka.file;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.WatchEvent;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.Map;
import java.util.Observable;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import static java.nio.file.StandardWatchEventKinds.*;

public class FileWatcher extends Observable {

  WatchService watcher;
  boolean recursive;
  Map<WatchKey, Path> directoriesByKey = new HashMap<WatchKey, Path>();
  
  Executor executor = Executors.newSingleThreadExecutor();
  
  FutureTask<Integer> task = new FutureTask<Integer>(new Callable<Integer>() {
    @Override
    public Integer call() {
      process();
      return 0;
    }
  });
  
  public FileWatcher(String dir, boolean recursive) {
    this(FileSystems.getDefault().getPath(dir), recursive);
  }
  
  public FileWatcher(Path path, boolean recursive) {
    try {
      this.recursive = recursive;
      FileSystem fileSystem = path.getFileSystem();
      watcher = fileSystem.newWatchService();
      create(path);
    } catch (IOException e) {
      throw new RuntimeException("");
    }
  }
  
  private void create(Path directory) {
    try {
      if (recursive) {
        registerAll(directory);
      } else {
        register(directory);
      }
    } catch (Exception e) {
      throw new RuntimeException("");
    }
  }
  
  public void execute() {
    executor.execute(task);
  }
  
  public void shutdown() throws IOException {
    watcher.close();
    executor = null;
  }
  
  private void process() {
    while (true) {
      WatchKey key;
      try {
        key = watcher.take();
      } catch (InterruptedException e) {
        return;
      }
      final Path dir = directoriesByKey.get(key);
      for (final WatchEvent<?> event : key.pollEvents()) {
        if (event.equals(OVERFLOW)) {
          continue;
        }
        notice(dir.toAbsolutePath().toString(), event);
        registerNewDirectory(dir, event);
      }
      boolean valid = key.reset();
      if (!valid) {
        directoriesByKey.remove(key);
        if (directoriesByKey.isEmpty()) {
          break;
        }
      }
    }
  }
  
  private void notice(String root, WatchEvent<?> event) {
    setChanged();
    String path = root + "/" + ((Path)event.context()).getFileName();
    System.out.println("---watcher---");
    System.out.println("Path : " + path);
    notifyObservers(new Info(path, event.kind()));
  }
  
  private void registerAll(Path rootDiretory) throws IOException {
    Files.walkFileTree(rootDiretory, new SimpleFileVisitor<Path>() {
      @Override
      public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
        register(dir);
        return FileVisitResult.CONTINUE;
      }
    });
  }
  
  private void register(Path path) throws IOException {
    WatchKey key = path.register(watcher, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);
    directoriesByKey.put(key, path);
  }
  
  @SuppressWarnings("unchecked")
  private void registerNewDirectory(Path dir, WatchEvent<?> event) {
    final Kind<?> kind = event.kind();
    if (recursive && kind.equals(ENTRY_CREATE)) {
      WatchEvent<Path> eventWithPath = (WatchEvent<Path>)event;
      Path current = eventWithPath.context();
      Path child = dir.resolve(current);
      try {
        if (Files.isDirectory(child, LinkOption.NOFOLLOW_LINKS)) {
          registerAll(child);
        }
      } catch (IOException e) {
        throw new RuntimeException("");
      }
    }
  }
  
  public static class Info {
    private final String path;
    private final Kind<?> kind;
    
    public Info(String path, Kind<?> kind) {
      this.path = path;
      this.kind = kind;
    }

    public String getPath() {
      return path;
    }

    public Kind<?> getKind() {
      return kind;
    }
  }
}

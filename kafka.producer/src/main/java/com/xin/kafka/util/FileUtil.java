package com.xin.kafka.util;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class FileUtil {

  private static final Charset defaultCharset = StandardCharsets.UTF_8;
  
  public static void write(Path path, List<String> lines) throws IOException {
    write(path, lines, defaultCharset);
  }
  
  public static void write(Path path, List<String> lines, Charset charset) throws IOException {
    if (lines != null) {
      Files.write(path, lines, charset, StandardOpenOption.CREATE);
    }
  }
  
  public static List<String> readLines(Path path) throws IOException {
    return readLines(path, defaultCharset);
  }
  
  public static List<String> readLines(Path path, Charset charset) throws IOException {
    return Files.readAllLines(path, charset);
  }
  
  public static void copy(Path source, Path target) throws IOException {
    Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);
  }
  
  public static List<Path> getChild(Path root, boolean recursive) throws IOException {
    final List<Path> pathes = new ArrayList<Path>();
    if (recursive) {
      Files.walkFileTree(root, new SimpleFileVisitor<Path>(){
        @Override
        public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) throws IOException {
          pathes.add(path);
          return FileVisitResult.CONTINUE;
        }
      });
    } else {
      Files.walkFileTree(root, Collections.<FileVisitOption>emptySet(), 1, new SimpleFileVisitor<Path>() {
        @Override
        public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) throws IOException {
          pathes.add(path);
          return FileVisitResult.CONTINUE;
        }
      });
    }
    return pathes;
  }
  
  public static void delete(Path path) throws IOException {
    Files.deleteIfExists(path);
  }
}

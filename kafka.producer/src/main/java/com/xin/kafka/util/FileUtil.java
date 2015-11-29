package com.xin.kafka.util;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.List;

import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.common.io.LineProcessor;

public class FileUtil {

  private static final Charset defaultCharset = Charset.forName("UTF-8");
  
  public static void write(Path path, String content) throws IOException {
    write(path, content, defaultCharset);
  }
  
  public static void write(Path path, String content, Charset charset) throws IOException {
    write(path.toFile(), content, charset);
  }
  
  public static void write(String path, String content) throws IOException {
    write(path, content, defaultCharset);
  }
  
  public static void write(String path, String content, Charset charset) throws IOException {
    write(new File(path), content, charset);
  }
  
  private static void write(File file, String content, Charset charset) throws IOException {
    Files.createParentDirs(file);
    Files.write(content, file, charset);
  }
  
  public static String read(Path path) throws IOException {
    return read(path, defaultCharset);
  }
  
  public static String read(Path path, Charset charset) throws IOException {
    return read(path.toFile(), charset);
  }
  
  public static String read(String path) throws IOException {
    return read(path, defaultCharset);
  }
  
  public static String read(String path, Charset charset) throws IOException {
    return read(new File(path), charset);
  }
  
  private static String read(File file, Charset charset) throws IOException {
    return Files.readLines(file, charset, new LineProcessor<String>() {
      
      private StringBuilder result = new StringBuilder();
      
      @Override
      public String getResult() {
        return result.toString();
      }

      @Override
      public boolean processLine(String line) throws IOException {
        result.append(line);
        return true;
      }});
  }
  
  public static List<String> read(Path path, final int startLine, final int maxLine) throws IOException {
    return read(path, startLine, maxLine, defaultCharset);
  }
  
  public static List<String> read(Path path, final int startLine, final int maxLine, Charset charset) throws IOException {
    return read(path.toFile(), startLine, maxLine, charset);
  }
  
  public static List<String> read(String path, final int startLine, final int maxLine) throws IOException {
    return read(path, startLine, maxLine, defaultCharset);
  }
  
  public static List<String> read(String path, final int startLine, final int maxLine, Charset charset) throws IOException {
    return read(new File(path), startLine, maxLine, charset);
  }
  
  private static List<String> read(File file, final int startLine, final int maxLine, Charset charset) throws IOException {
    return Files.readLines(file, charset, new LineProcessor<List<String>>() {

      List<String> lines = Lists.newArrayList();
      int lineNum = 0;
      
      @Override
      public List<String> getResult() {
        return lines;
      }

      @Override
      public boolean processLine(String line) throws IOException {
        lineNum++;
        if (lineNum < startLine) {
          return true;
        } else if (lineNum >= startLine && lineNum < startLine + maxLine) {
          lines.add(line);
        }
        return false;
      }});
  }
  
  public static void copy(String from, String to) throws IOException {
    Files.copy(new File(from), new File(to));
  }
  
  public static List<File> getChild(File root, boolean recursive) {
    List<File> files = Lists.newArrayList();
    for (File file : root.listFiles()) {
      if (file.isFile()) {
        files.add(file);
      } else if (file.isDirectory() && recursive) {
        files.addAll(getChild(file, recursive));
      }
    }
    return files;
  }
}

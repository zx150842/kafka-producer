package com.xin.kafka.file;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.commons.lang3.math.NumberUtils;

import com.xin.kafka.util.FileUtil;

public class CheckPoint {

  public static int get(String path) {
    int point;
    try {
      point = NumberUtils.toInt(FileUtil.read(path), 0);
    } catch (IOException e) {
      System.out.println("Create checkpoint 0 : " + path);
      point = 0;
      persist(path, point);
    }
    return point;
  }
  
  public static void persist(String path, int point) {
    try {
      FileUtil.write(path, String.valueOf(point));
    } catch (IOException e) {
      System.out.println("Cannot find checkPoint : " + path);
      throw new RuntimeException(String.format("Cannot create file %s. ", path), e);
    }
  }
  
  public static void save2TmpFile(String path) {
    rename(path, path + "-tmp");
  }
  
  public static void rename(String path, String newPath) {
    try {
      FileUtil.copy(path, newPath);
    } catch (IOException e) {
      throw new RuntimeException(String.format("Cannot copy file %s to %s", path, newPath), e);
    }
  }
  
  public static void remove(String path) {
    try {
      Files.deleteIfExists(Paths.get(path));
    } catch (IOException e) {
      System.out.println("Cannot delete checkpoint : " + path);
      // do nothing
    }
  }
}

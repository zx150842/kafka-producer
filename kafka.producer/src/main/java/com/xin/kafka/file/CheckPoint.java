package com.xin.kafka.file;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.xin.kafka.util.FileUtil;

public class CheckPoint {

  static final Logger log = LoggerFactory.getLogger(CheckPoint.class);
  
  public static int get(Path checkPointPath, Path logFilePath) {
    int point;
    try {
      List<String> lines = FileUtil.readLines(checkPointPath);
      point = NumberUtils.toInt(lines.get(1), 0);
    } catch (IOException e) {
      log.info("[CheckPoint]Cannot get checkpoint, path : {}. Create checkpoint 0, point log path : {}. ", checkPointPath, logFilePath);
      point = 0;
      persist(checkPointPath, logFilePath, point);
    }
    return point;
  }
  
  public static void persist(Path checkPointPath, Path logFilePath, int point) {
    try {
      List<String> content = Lists.newArrayList();
      content.add(logFilePath.toString());
      content.add(String.valueOf(point));
      FileUtil.write(checkPointPath, content);
    } catch (IOException e) {
      log.error("[CheckPoint]Cannot find checkPoint, path : {}, error : {}. ", checkPointPath, e);
    }
  }
  
  public static void save2TmpFile(Path checkPointPath) {
    rename(checkPointPath, Paths.get(checkPointPath + "-tmp"));
  }
  
  public static void rename(Path orgPath, Path newPath) {
    try {
      FileUtil.copy(orgPath, newPath);
    } catch (IOException e) {
      log.error("[CheckPoint]Cannot copy file {} to {}, error : {}. ", orgPath, newPath, e);
    }
  }
  
  public static void remove(Path checkPointPath) {
    try {
      FileUtil.delete(checkPointPath);
    } catch (IOException e) {
      log.error("[CheckPoint]Cannot delete checkpoint, path : {}, error : {}. ", checkPointPath, e);
    }
  }
}

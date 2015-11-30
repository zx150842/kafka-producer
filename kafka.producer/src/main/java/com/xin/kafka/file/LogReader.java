package com.xin.kafka.file;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class LogReader {
  
  static final Logger log = LoggerFactory.getLogger(LogReader.class);
  
  private int maxLength;
  private byte[] bytes;
  private byte[] tmpBytes;
  private ByteBuffer byteBuffer;
  private ByteBuffer tmpByteBuffer;
  private FileChannel fileChannel;
  protected int start;
  
  private String incompleteLine;
  
  public LogReader(Configuration conf) {
    maxLength = conf.getMaxSize();
    bytes = new byte[maxLength];
    tmpBytes = new byte[maxLength];
    byteBuffer = ByteBuffer.wrap(bytes);
    tmpByteBuffer = ByteBuffer.wrap(tmpBytes);
  }
  
  public void prepare(Path path) {
    try {
      fileChannel = FileChannel.open(path, StandardOpenOption.READ);
    } catch (IOException e) {
      log.error("[LogReader]Cannot open fileChannel on path : {}, error : {}. ", path, e);
    }
  }
  
  public List<String> readLines() throws IOException {
    List<String> lines = Lists.newArrayList();
    fileChannel.position(start);
    int num = fileChannel.read(byteBuffer);
    if (num > 0) {
      byteBuffer.flip();
      while (byteBuffer.hasRemaining()) {
        byte currentByte = byteBuffer.get();
        if (currentByte == '\r') {
          String line = getLine(tmpByteBuffer);
          if (StringUtils.isNotBlank(line)) {
            lines.add(line);
          }
        } else if (currentByte == '\n') {
          String line = getLine(tmpByteBuffer);
          if (StringUtils.isNotBlank(line)) {
            lines.add(line);
          }
        } else {
          tmpByteBuffer.put(currentByte);
        }
      }
      incompleteLine = getLine(tmpByteBuffer);
      start += byteBuffer.limit();
      byteBuffer.clear();
    } else if (num == 0) {
      // do nothing
    } else {
      if (StringUtils.isNotBlank(incompleteLine)) {
        lines.add(incompleteLine);
        incompleteLine = null;
      }
    }
    return lines;
  }
  
  private String getLine(ByteBuffer byteBuffer) {
    String line = null;
    if (byteBuffer.position() > 0) {
      tmpByteBuffer.flip();
      line = new String(tmpByteBuffer.array(), 0, tmpByteBuffer.limit());
      if (incompleteLine != null) {
        line = incompleteLine.concat(line);
        incompleteLine = null;
      } 
      tmpByteBuffer.clear();
    }
    return line;
  }
  
}

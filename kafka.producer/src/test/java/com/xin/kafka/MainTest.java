package com.xin.kafka;

import org.junit.Test;

import com.xin.kafka.file.Configuration;

public class MainTest {

  @Test
  public void test() throws InterruptedException {
    Main main = new Main();
    String logFilePath = "E:\\test";
    String checkPointPath = "E:\\checkpoint";
    Configuration conf = new Configuration.Builder(checkPointPath).suffixTimestampRegex("[0-9]{12}").build();
    main.execute(conf, logFilePath, checkPointPath);
    Thread.sleep(40000000);
  }
}

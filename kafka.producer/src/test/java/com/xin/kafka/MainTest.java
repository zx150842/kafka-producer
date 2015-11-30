package com.xin.kafka;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Test;

import com.xin.kafka.file.Configuration;

public class MainTest {

  @Test
  public void test() throws InterruptedException {
    Main main = new Main();
    Path logFilePath = Paths.get("D:\\test\\test");
    Path checkPointPath = Paths.get("D:\\test\\checkpoint");
    Configuration conf = new Configuration.Builder(logFilePath, checkPointPath).suffixTimestampRegex("[0-9]{12}").build();
    main.execute(conf);
    Thread.sleep(40000000);
  }
  
  @Test
  public void test1() {
    Pattern pattern = Pattern.compile("[0-9]{12}$");
    Matcher matcher = pattern.matcher("D:\\test\\test\\history\\test_151202135201");
    while (matcher.find()) {
      System.out.println("find");
      return;
    }
    System.out.println("not find");
  }
}

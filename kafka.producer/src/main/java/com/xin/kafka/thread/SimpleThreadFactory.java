package com.xin.kafka.thread;

import java.nio.file.Path;

public class SimpleThreadFactory {

  public Thread newThread(Runnable r) {
    Thread thread = new Thread(r);
    return thread;
  }
}

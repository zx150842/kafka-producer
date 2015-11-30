package com.xin.kafka.thread;

public class SimpleThreadFactory {

  public Thread newThread(Runnable r) {
    Thread thread = new Thread(r);
    return thread;
  }
}

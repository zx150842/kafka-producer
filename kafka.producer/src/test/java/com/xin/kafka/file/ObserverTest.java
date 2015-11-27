package com.xin.kafka.file;

import java.util.Observable;
import java.util.Observer;

import org.junit.Test;

public class ObserverTest {

  @Test
  public void test() {
    Producer p = new Producer();
    Consumer consumer = new Consumer();
    p.addObserver(consumer);
    p.publish("publish a msg");
  }
  
  public class Producer extends Observable {
    String magazine;
    public void publish(String msg) {
      this.magazine = msg;
      setChanged();
      notifyObservers();
    }
    public String getMagazine() {
      return magazine;
    }
  }
  
  public class Consumer implements Observer {

    public void update(Observable o, Object arg) {
      Producer p = (Producer) o;
      System.out.println("get msg " + p.getMagazine());
    }
  }
}

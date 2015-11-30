package com.xin.kafka.util;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Properties;

public class Resources {

  private static ClassLoaderWrapper classLoaderWrapper = new ClassLoaderWrapper();
  private static Charset charset;
  
  public Resources() {
  }
  
  public static URL getResourceAsUrl(String resource) throws IOException {
    return getResourceAsUrl(resource, null);
  }
  
  public static URL getResourceAsUrl(String resource, ClassLoader classLoader) throws IOException {
    URL url = classLoaderWrapper.getResourceAsUrl(resource);
    if (url == null) {
      throw new IOException("Cannot load resource : " + resource + ", classLoader : " + classLoader);
    }
    return url;
  }
  
  public static File getResourceAsFile(String resource) throws IOException {
    return new File(getResourceAsUrl(resource).getFile());
  }
  
  public static File getResourceAsFile(String resource, ClassLoader classLoader) throws IOException {
    return new File(getResourceAsUrl(resource, classLoader).getFile());
  }
  
  public static InputStream getResourceAsStream(String resource) throws IOException {
    return getResourceAsStream(resource, null);
  }
  
  public static InputStream getResourceAsStream(String resource, ClassLoader classLoader) throws IOException {
    InputStream in = classLoaderWrapper.getResourceAsStream(resource, classLoader);
    if (null == in) {
      throw new IOException("Cannot load resource : " + resource + ", classLoader : " + classLoader);
    }
    return in;
  }
  
  public static Properties getResourceAsProperties(String resource) throws IOException {
    InputStream in = getResourceAsStream(resource);
    return getResourceAsProperties(in);
  }
  
  public static Properties getResourceAsProperties(String resource, ClassLoader classLoader) throws IOException {
    InputStream in = getResourceAsStream(resource, classLoader);
    return getResourceAsProperties(in);
  }
  
  private static Properties getResourceAsProperties(InputStream in) throws IOException {
    Properties properties = new Properties();
    properties.load(in);
    return properties;
  }
  
  public static Reader getResourceAsReader(String resource) throws IOException {
    InputStream in = getResourceAsStream(resource);
    return new InputStreamReader(in);
  }
  
  public static Reader getResourceAsReader(String resource, ClassLoader classLoader) throws IOException {
    InputStream in = getResourceAsStream(resource, classLoader);
    return new InputStreamReader(in);
  }
  
  public static Class<?> classForName(String name) {
    return classLoaderWrapper.classForName(name);
  }
  
  public static Class<?> classForName(String name, ClassLoader classLoader) {
    return classLoaderWrapper.classForName(name, classLoader);
  }

  public static Charset getCharset() {
    return charset;
  }

  public static void setCharset(Charset charset) {
    Resources.charset = charset;
  }
}

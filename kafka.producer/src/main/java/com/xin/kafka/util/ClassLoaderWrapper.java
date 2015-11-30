package com.xin.kafka.util;

import java.io.InputStream;
import java.net.URL;

public class ClassLoaderWrapper {
  
  private ClassLoader systemClassLoader;

  public ClassLoaderWrapper() {
    systemClassLoader = ClassLoader.getSystemClassLoader();
  }
  
  public URL getResourceAsUrl(String resource) {
    return getResourceAsUrl(resource, getClassLoaders(null));
  }
  
  public URL getResourceAsUrl(String resource, ClassLoader classLoader) {
    return getResourceAsUrl(resource, getClassLoaders(classLoader));
  }
  
  public InputStream getResourceAsStream(String resource) {
    return getResourceAsStream(resource, getClassLoaders(null));
  }
  
  public InputStream getResourceAsStream(String resource, ClassLoader classLoader) {
    return getResourceAsStream(resource, getClassLoaders(classLoader));
  }
  
  public Class<?> classForName(String name) {
    return classForName(name, getClassLoaders(null));
  }
  
  public Class<?> classForName(String name, ClassLoader classLoader) {
    return classForName(name, getClassLoaders(classLoader));
  }
  
  private Class<?> classForName(String name, ClassLoader[] classLoaders) {
    for (ClassLoader classLoader : classLoaders) {
      if (null != classLoader) {
        try {
          Class<?> clazz = Class.forName(name, true, classLoader);
          if (null != clazz) {
            return clazz;
          }
        } catch (ClassNotFoundException e) {
        }
      }
    }
    throw new RuntimeException("Could not load class, name = " + name);
  }
  
  private InputStream getResourceAsStream(String resource, ClassLoader[] classLoaders) {
    for (ClassLoader classLoader : classLoaders) {
      if (null != classLoader) {
        InputStream in = classLoader.getResourceAsStream(resource);
        if (null != in) {
          return in;
        }
        in = classLoader.getResourceAsStream("/" + resource);
        if (null != in) {
          return in;
        }
      }
    }
    return null;
  }
  
  private ClassLoader[] getClassLoaders(ClassLoader classLoader) {
    return new ClassLoader[]{
        classLoader,
        Thread.currentThread().getContextClassLoader(),
        getClass().getClassLoader(),
        systemClassLoader
    };
  }
  
  private URL getResourceAsUrl(String resource, ClassLoader[] classLoaders) {
    for (ClassLoader classLoader : classLoaders) {
      if (null != classLoader) {
        URL url = classLoader.getResource(resource);
        if (null != url) {
          return url;
        }
      }
    }
    return null;
  }
}

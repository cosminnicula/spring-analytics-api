package dev.intermediatebox.analyticsapi.application;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

@Component
public class SpringContext implements ApplicationContextAware {
  private static ApplicationContext context;

  // Returns the Spring managed bean instance of the given class type if it exists, null otherwise.
  public static <T extends Object> T getBean(Class<T> beanClass) {
    return context.getBean(beanClass);
  }

  public static String getProperty(String propertyName) {
    return context.getEnvironment().getProperty(propertyName);
  }

  @Override
  public void setApplicationContext(ApplicationContext context) throws BeansException {
    // store ApplicationContext reference to access required beans later on
    SpringContext.context = context;
  }
}
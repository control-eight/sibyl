package com.my.sibyl.itemsets.guice;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;

import javax.inject.Inject;

/**
 * @author abykovsky
 * @since 6/9/15
 */
public class MonitorInterceptor implements MethodInterceptor {

    @Inject
    private MonitorService monitorService;

    public Object invoke(MethodInvocation methodInvocation) throws Throwable {
        monitorService.addMethodInvocation(methodInvocation.getMethod().toString());
        return methodInvocation.proceed();
    }
}

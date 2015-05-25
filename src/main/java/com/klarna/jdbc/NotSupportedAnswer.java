package com.klarna.jdbc;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Created by aonuchin on 16.01.15.
 */
public class NotSupportedAnswer implements Answer {
    @Override
    public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
        throw new UnsupportedOperationException("Operation "
                + invocationOnMock.getMethod().toGenericString()
                + " is not supported");
    }
}

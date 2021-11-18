package org.apache.fineract.infrastructure.core.messaging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.ErrorHandler;

public class DefaultErrorHandler implements ErrorHandler {

    private static Logger LOG = LoggerFactory.getLogger(DefaultErrorHandler.class);

    @Override
    public void handleError(Throwable t) {
        LOG.error(t.getCause().getMessage());
    }
}

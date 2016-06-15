package org.everthrift.appserver.utils.tg;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

interface Clock {
    long currentTime();
}

class SystemClock implements Clock {
    @Override
    public long currentTime() {
        return System.currentTimeMillis();
    }
}

abstract class AbstractMonotonicTimestampGenerator implements TimestampGenerator {
    private static final Logger logger = LoggerFactory.getLogger(AbstractMonotonicTimestampGenerator.class);

    volatile Clock clock = new SystemClock();

    protected long computeNext(long last) {
        long millis = last / 1000;
        long counter = last % 1000;

        long now = clock.currentTime();

        // System.currentTimeMillis can go backwards on an NTP resync, hence the ">" below
        if (millis >= now) {
            if (counter == 999)
                logger.warn("Sub-millisecond counter overflowed, some query timestamps will not be distinct");
            else
                counter += 1;
        } else {
            millis = now;
            counter = 0;
        }

        return millis * 1000 + counter;
    }
}

package com.knockchat.utils.tg;

import java.util.concurrent.atomic.AtomicLong;

public class AtomicMonotonicTimestampGenerator extends AbstractMonotonicTimestampGenerator {
    private AtomicLong lastRef = new AtomicLong(0);

    @Override
    public long next() {
        while (true) {
            long last = lastRef.get();
            long next = computeNext(last);
            if (lastRef.compareAndSet(last, next))
                return next;
        }
    }
}

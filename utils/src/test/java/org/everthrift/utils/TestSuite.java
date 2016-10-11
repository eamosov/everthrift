package org.everthrift.utils;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by fluder on 23.09.16.
 */
public class TestSuite {

    @Test
    public void testUuidSpace() {
        assertEquals(UUID.getSpace8bit("0a000000-0000-0000-c501-5741efe34dec"), 10);
    }
}

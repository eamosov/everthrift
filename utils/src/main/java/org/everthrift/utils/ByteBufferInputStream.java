package org.everthrift.utils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * Created by fluder on 25.10.16.
 */
public class ByteBufferInputStream extends InputStream {
    ByteBuffer bb;

    public ByteBufferInputStream(ByteBuffer var1) {
        this.bb = var1;
    }

    public int read() throws IOException {
        if(this.bb == null) {
            throw new IOException("read on a closed InputStream");
        } else {
            return this.bb.remaining() == 0?-1:this.bb.get() & 255;
        }
    }

    public int read(byte[] var1) throws IOException {
        if(this.bb == null) {
            throw new IOException("read on a closed InputStream");
        } else {
            return this.read(var1, 0, var1.length);
        }
    }

    public int read(byte[] var1, int var2, int var3) throws IOException {
        if(this.bb == null) {
            throw new IOException("read on a closed InputStream");
        } else if(var1 == null) {
            throw new NullPointerException();
        } else if(var2 >= 0 && var3 >= 0 && var3 <= var1.length - var2) {
            if(var3 == 0) {
                return 0;
            } else {
                int var4 = Math.min(this.bb.remaining(), var3);
                if(var4 == 0) {
                    return -1;
                } else {
                    this.bb.get(var1, var2, var4);
                    return var4;
                }
            }
        } else {
            throw new IndexOutOfBoundsException();
        }
    }

    public long skip(long var1) throws IOException {
        if(this.bb == null) {
            throw new IOException("skip on a closed InputStream");
        } else if(var1 <= 0L) {
            return 0L;
        } else {
            int var3 = (int)var1;
            int var4 = Math.min(this.bb.remaining(), var3);
            this.bb.position(this.bb.position() + var4);
            return (long)var3;
        }
    }

    public int available() throws IOException {
        if(this.bb == null) {
            throw new IOException("available on a closed InputStream");
        } else {
            return this.bb.remaining();
        }
    }

    public void close() throws IOException {
        this.bb = null;
    }

    public synchronized void mark(int var1) {
    }

    public synchronized void reset() throws IOException {
        throw new IOException("mark/reset not supported");
    }

    public boolean markSupported() {
        return false;
    }
}

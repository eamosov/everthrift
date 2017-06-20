package org.everthrift.utils.image;

public class ImageImpl implements ImageIF {

    final int width;

    final int height;

    final String format;

    final byte[] data;

    public ImageImpl(int width, int height, String format, byte[] data) {
        super();
        this.width = width;
        this.height = height;
        this.format = format;
        this.data = data;
    }

    @Override
    public int getWidth() {
        return width;
    }

    @Override
    public int getHeight() {
        return height;
    }

    @Override
    public String getFormat() {
        return format;
    }

    @Override
    public byte[] getData() {
        return data;
    }
}

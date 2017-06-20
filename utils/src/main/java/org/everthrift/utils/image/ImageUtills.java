package org.everthrift.utils.image;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class ImageUtills {

    private static Logger LOG = LoggerFactory.getLogger(ImageUtills.class);

    public enum ImageFormat {
        JPEG(new byte[]{(byte) 0xFF, (byte) 0xD8}),
        JPEG_JFIF(new byte[]{0x4A, 0x46, 0x49, 0x46}),
        PNG(new byte[]{(byte) 0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A}),
        GIF89(new byte[]{0x47, 0x49, 0x46, 0x38, 0x39, 0x61}),
        GIF87(new byte[]{0x47, 0x49, 0x46, 0x38, 0x37, 0x61});

        private final byte[] startBytes;

        ImageFormat(byte[] bytes) {
            this.startBytes = bytes;
        }
    }

    /**
     * @param imgArr массив байтов изображения
     * @param format см <code>ImageFormat</code>
     * @return
     * @throws NullPointerException если <code>imgArr</code>==<code>null</code>
     */
    public static boolean testImgFormat(byte[] imgArr, ImageFormat format) throws NullPointerException {
        if (imgArr == null) {
            throw new NullPointerException("image array is null");
        }
        return Arrays.equals(Arrays.copyOf(imgArr, format.startBytes.length), format.startBytes);
    }

}

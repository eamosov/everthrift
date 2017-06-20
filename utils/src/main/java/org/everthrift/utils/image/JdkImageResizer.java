package org.everthrift.utils.image;

import org.imgscalr.Scalr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.IIOImage;
import javax.imageio.ImageIO;
import javax.imageio.ImageWriteParam;
import javax.imageio.ImageWriter;
import javax.imageio.plugins.jpeg.JPEGImageWriteParam;
import javax.imageio.stream.MemoryCacheImageOutputStream;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class JdkImageResizer implements ImageResizerIF {

    private static final Logger log = LoggerFactory.getLogger(JdkImageResizer.class);

    @Override
    public ImageIF resizeImg(byte[] input, int targtWeight, int targetHeight, int quality, boolean noZoomIn) throws ImageException {

        final long start = System.currentTimeMillis();
        try {
            final BufferedImage img = ImageIO.read(new ByteArrayInputStream(input));

            if (img.getWidth() <= targtWeight && img.getHeight() <= targetHeight && noZoomIn) {
                return new ImageImpl(img.getWidth(), img.getHeight(), "jpg", input);
            }

            final BufferedImage resized = Scalr.resize(img, Scalr.Method.ULTRA_QUALITY, Scalr.Mode.AUTOMATIC, targtWeight, targetHeight);
            final ByteArrayOutputStream bos = new ByteArrayOutputStream();
            // ImageIO.write(resized, ImageFormat.JPEG.name(), bos);

            final ImageWriter writer = ImageIO.getImageWritersByFormatName("jpg").next();
            writer.setOutput(new MemoryCacheImageOutputStream(bos));
            JPEGImageWriteParam jpegParams = new JPEGImageWriteParam(null);
            jpegParams.setCompressionMode(ImageWriteParam.MODE_EXPLICIT);
            jpegParams.setCompressionQuality((float) (quality / 100.0));
            writer.write(null, new IIOImage(resized, null, null), jpegParams);

            return new ImageImpl(resized.getWidth(), resized.getHeight(), "jpg", bos.toByteArray());
        } catch (IOException e) {
            throw new ImageException(e);
        } finally {
            final long end = System.currentTimeMillis();
            log.debug("resizeImg took {} ms", (end - start));
        }
    }

}

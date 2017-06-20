package org.everthrift.utils.image;

import magick.ImageInfo;
import magick.Magick;
import magick.MagickException;
import magick.MagickImage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ImagickImageResizer extends Magick implements ImageResizerIF {

    private static final Logger log = LoggerFactory.getLogger(ImagickImageResizer.class);

    @Override
    public ImageIF resizeImg(byte[] input, int targtWeight, int targetHeight, int quality, boolean noZoomIn) throws ImageException {
        final long start = System.currentTimeMillis();

        try {
            final ImageInfo origInfo = new ImageInfo();
            MagickImage lightImg = new MagickImage(origInfo, input);

            // Get present dimensions
            int origW = (int) lightImg.getDimension().getWidth();
            int origH = (int) lightImg.getDimension().getHeight();

            double kW = (double) targtWeight / (double) origW;
            double kH = (double) targetHeight / (double) origH;
            double k = kW < kH ? kW : kH;

            if (k >= 1 && noZoomIn) // no zoom out
            {
                return new ImageImpl(origW, origH, lightImg.getImageFormat(), input);
            }

            lightImg = lightImg.scaleImage((int) (origW * k), (int) (origH * k));
            lightImg.setImageFormat("jpg");
            lightImg.setQuality(quality);
            final byte data[] = lightImg.imageToBlob(new ImageInfo());

            return new ImageImpl((int) lightImg.getDimension().getWidth(), (int) lightImg.getDimension()
                                                                                         .getHeight(), "jpg", data);

        } catch (MagickException e) {
            throw new ImageException(e);
        } finally {
            final long end = System.currentTimeMillis();
            log.debug("resizeImg took {} ms", (end - start));
        }
    }

}

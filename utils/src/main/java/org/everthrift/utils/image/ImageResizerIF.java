package org.everthrift.utils.image;

public interface ImageResizerIF {

    public ImageIF resizeImg(byte[] input, int targtWeight, int targetHeight, int quality, boolean noZoomIn) throws ImageException;

}

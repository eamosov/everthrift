package org.everthrift.utils.image;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ImageResizerFactory {

    private static final Logger log = LoggerFactory.getLogger(ImageResizerFactory.class);

    private static volatile ImageResizerIF resizer;

    public static ImageResizerIF getInstance() {
        ImageResizerIF _resizer = resizer;
        if (_resizer == null) {
            synchronized (ImageResizerFactory.class) {
                _resizer = resizer;
                if (_resizer == null) {
                    try {
                        final Class<ImageResizerIF> cls = (Class) Class.forName("com.knockchat.node.utils.image.ImagickImageResizer");
                        _resizer = cls.newInstance();
                        log.info("using ImagickImageResizer");
                    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | UnsatisfiedLinkError e) {
                        log.error("Error loading ImagickImageResizer", e);
                        _resizer = new JdkImageResizer();
                        log.info("using JdkImageResizer");
                    }
                    resizer = _resizer;
                }
            }
        }
        return _resizer;
    }
}

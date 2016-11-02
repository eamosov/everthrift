package org.everthrift.utils;

/**
 * Created by fluder on 01.11.16.
 */
public class ExceptionUtils {

    public interface I<T> {
        T apply() throws Exception;
    }

    public interface V {
        void apply() throws Exception;
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> void throwException(Throwable exception, Object dummy) throws T {
        throw (T) exception;
    }

    private static void throwException(Throwable exception) {
        ExceptionUtils.<RuntimeException>throwException(exception, null);
    }

    public static <T> T asUnchecked(I<T> i) {
        try {
            return i.apply();
        } catch (Exception e) {
            ExceptionUtils.throwException(e);
            return null;
        }
    }

    public static void asUnchecked(V i) {
        try {
            i.apply();
        } catch (Exception e) {
            ExceptionUtils.throwException(e);
        }
    }
}

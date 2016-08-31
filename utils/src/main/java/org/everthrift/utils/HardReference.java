package org.everthrift.utils;

/**
 * Created by fluder on 31.08.16.
 */
public class HardReference<T> {

    private T value;

    private HardReference(){

    }

    public static <T> HardReference<T> create(){
        return new HardReference<>();
    }

    public static <T> HardReference<T> of(T value){
        final HardReference<T> r = new HardReference<>();
        r.set(value);
        return r;
    }

    public T get() {
        return value;
    }

    public void set(T value) {
        this.value = value;
    }
}

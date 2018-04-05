package com.akbarseptriyan.app;

import java.util.function.Consumer;

public class DbzFunction implements Consumer {

    @Override
    public void accept(Object o) {
        System.out.println(o.toString());
    }
}

package com.demo.gmall.dw.mocker.utils;

/**
 * @aythor HeartisTiger
 * 2019-02-22 18:50
 */
public class RanOpt<T>{
    T value ;
    int weight;

    public RanOpt ( T value, int weight ){
        this.value=value ;
        this.weight=weight;
    }

    public T getValue() {
        return value;
    }

    public int getWeight() {
        return weight;
    }
}
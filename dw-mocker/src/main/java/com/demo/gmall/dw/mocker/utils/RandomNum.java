package com.demo.gmall.dw.mocker.utils;

import java.util.Random;

/**
 * @aythor HeartisTiger
 * 2019-02-22 18:53
 */
public class RandomNum {
    public static final  int getRandInt(int fromNum,int toNum){
        return   fromNum+ new Random().nextInt(toNum-fromNum+1);
    }
}

package com.cfp.runners.entity;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public class InternalObject {

    private String id;
    private Integer randomNumber;
    private String randomString;
    private String mypk;

    public InternalObject() {
    }

    public static InternalObject createInternalObject(String randomString) {
        InternalObject internalObject = new InternalObject();

        internalObject.id = UUID.randomUUID().toString();
        internalObject.randomNumber = ThreadLocalRandom.current().nextInt(1000);
        internalObject.randomString = randomString;
        internalObject.mypk = UUID.randomUUID().toString();

        return internalObject;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setRandomNumber(Integer randomNumber) {
        this.randomNumber = randomNumber;
    }

    public void setRandomString(String randomString) {
        this.randomString = randomString;
    }

    public void setMypk(String mypk) {
        this.mypk = mypk;
    }

    public String getId() {
        return id;
    }

    public Integer getRandomNumber() {
        return randomNumber;
    }

    public String getRandomString() {
        return randomString;
    }

    public String getMypk() {
        return mypk;
    }
}

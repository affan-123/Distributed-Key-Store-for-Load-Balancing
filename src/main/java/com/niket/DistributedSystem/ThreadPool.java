package com.niket.DistributedSystem;

public interface ThreadPool{
    public void addToQueue(Runnable r) throws InterruptedException;
}
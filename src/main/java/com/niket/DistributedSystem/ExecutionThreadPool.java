package com.niket.DistributedSystem;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Executors;

import com.niket.DistributedSystem.ThreadPool;

public class ExecutionThreadPool implements ThreadPool{

    private static ExecutionThreadPool single_instance = null;
    private static ExecutionThreadPool single_instance_fix = null;
    public ExecutorService executor;

    public static ExecutionThreadPool getInstance(){
        if (single_instance == null ){
            single_instance = new ExecutionThreadPool();
        }
        return single_instance;
    }

    public static ExecutionThreadPool getInstance(int size){
        if (single_instance_fix == null ){
            single_instance_fix = new ExecutionThreadPool(size);
        }
        return single_instance_fix;
    }

    private ExecutionThreadPool(){
        executor = Executors.newCachedThreadPool();
    }

    private ExecutionThreadPool(int poolSize){
        executor = Executors.newFixedThreadPool(poolSize);
    }

    public ExecutionThreadPool(int poolSize, int maxPoolSize){
        executor = new ThreadPoolExecutor(poolSize, maxPoolSize,
                                      0L, TimeUnit.MILLISECONDS,
                                      new LinkedBlockingQueue<Runnable>());
    }

    

    @Override
    public void addToQueue(Runnable task) throws InterruptedException{
        executor.submit(task);

    }

}
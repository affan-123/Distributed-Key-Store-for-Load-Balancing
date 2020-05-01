/*

Author : Niket Doke
Date : 10 Feb 2020

This class stores Key Value pairs into database. Every Value is stored with 
a version which is incremented everytime the key is updated Provides get,put 
and delete method for KVStore.

*/
package com.niket.DistributedSystem;

import java.util.Map;

import com.niket.DistributedSystem.KVStore;

public class KVServer{

    private static KVServer single_instance = null;
    KVStore _store;
    int read;
    int write;
    int replica;
    //cache

    private KVServer(){
        _store = new KVStore();
        this.read = this.write = this.replica = 1;
    }

    private KVServer(int read, int write, int replica){
        this.read = read;
        this.write = write;
        this.replica = replica;
        _store = new KVStore();
    }

    public int getReadCount(){
        return this.read;
    }

    public int getWriteCount(){
        return this.write;
    }

    public int getReplicaCount(){
        return this.replica;
    }

    public static KVServer getInstance(){
        if(single_instance == null){
            single_instance = new KVServer();
        }
        return single_instance;
    }

    public static KVServer getInstance(int read, int write, int replica){
        if(single_instance == null){
            single_instance = new KVServer(read, write, replica);
        }
        return single_instance;
    }

    public boolean put(String key, String value){

        //cache operations
        _store.put(key,value);
        return true;
    }

    public String get(String key){

        //cache operations
        String value = _store.get(key);
        return value;
    }
    
    public boolean delete(String key){
        
        return _store.delete(key);
    }

    public Map<String,String> getKeysInRange(int low, int high, int size){
        
        return _store.getKeysInRange(low,high,size);

        
    }
}
/*

Author : Niket Doke
Date : 10 Feb 2020

This class stores Key Value pairs into database. Every Value is stored with 
a version which is incremented everytime the key is updated Provides get,put 
and delete method for KVStore.

*/
package com.niket.DistributedSystem;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class KVStore{

    class Value{
        String value;
        int version;

        Value(String value,int version){
            this.value = value;
            this.version = version;
        }

        public int getVersion(){
            return this.version;
        }

        public String getValue(){
            return this.value;
        }

        
    }

    private ConcurrentHashMap<String,Value> _db = new ConcurrentHashMap<>();

    public void serve(){

    }                                                                                                            

    public boolean put(String key,String value){

        Value preValue = _db.get(key);
        Value valueObject  = null;
        
        if( preValue == null){
            valueObject = new Value(value,0);
        } else {
            valueObject = new Value(value,preValue.getVersion()+1);
        }
        _db.put(key,valueObject);
        debug();
        return true;
    }

    public String get(String key){

        Value value = _db.get(key);

        if(value != null){
            return value.getValue()+"#"+value.getVersion();
        } else{
            return null;
        }
    }

    public void debug(){
        for(Map.Entry<String,Value> entry : this._db.entrySet()){
            
            System.out.println("Key : "+entry.getKey()+" Value:"+entry.getValue().getValue());
        }
    }

    public boolean delete(String key){

        _db.remove(key);
        debug();
        return true;
    }
    private  int getHash(String key,int size){
        ConsistentHashingCoordinator consistentHashingCoordinator = ConsistentHashingCoordinator.getInstance();
        return consistentHashingCoordinator.getHash(key);
        //return (int) (hashFunction.hash(key ) % size);
    }

    

    public Map<String,String> getKeysInRange(int low, int high,int size){

        Map<String,String> map = new HashMap<>();
       // MD5Hash hashFunction = new MD5Hash();

        for(Map.Entry<String,Value> entry : this._db.entrySet()){
            int hash = getHash(entry.getKey()   , size);
            System.out.println("Hash for key "+entry.getKey()+" is "+hash);
            if( hash >=low && hash < high ){
                map.put(entry.getKey(), entry.getValue().getValue());
                System.out.println("Hence adding");
            }

        }
        return map;

    }

}
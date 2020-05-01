package com.niket.DistributedSystem;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import com.niket.DistributedSystem.MD5Hash;
import com.niket.DistributedSystem.Node;




public class ConsistentHashing{

    private MD5Hash hashFunction;
    private VanEmdeBoasMap<Node> ring ;
    private int size;
    private int replicaCount;
    

    public ConsistentHashing(int size, int replicaCount){
        this.size = size;
        this.replicaCount = replicaCount;
        this.hashFunction = new MD5Hash();
        this.ring = new VanEmdeBoasMap<>(size);
    }

    public ConsistentHashing(int size, int replicaCount, Collection<Node> nodeList){
        this.size = size;
        this.replicaCount = replicaCount;
        this.hashFunction = new MD5Hash();
        this.ring = new VanEmdeBoasMap<>(size);

        for(Node node : nodeList){
            this.addNode(node);
        }
    }

    public boolean addNode(Node node){
        
        try{
            for(int i = 0; i < this.replicaCount; i++){
                int hash = (int) (hashFunction.hash(node.getID() + Integer.toString(i) ) % size);
                this.ring.put(hash, node);
            }
            return true;
        } catch(Exception e){
            System.out.println("Exception while adding node "+ e);
            return false;
        }    
        
    }

    public int size(){
        return this.size;
    }


    public void debug(){
        System.out.println("Printing node List in the ring");
        for(Map.Entry<Integer,Node> entry : this.ring.entrySet()){
            System.out.println("Key : "+entry.getKey()+" NOde : "+entry.getValue());
        }
     
    }

    public  int getHash(String key){

        return (int) (hashFunction.hash(key ) % size);
    }

    public boolean deleteNode(Node node){
        
        try{
            for(int i = 0; i < this.replicaCount; i++){
                int hash = (int) (hashFunction.hash(node.getID() + Integer.toString(i) ) % size);
                if(this.ring.get(hash).equals(node)){
                    this.ring.remove(hash);
                } 
            }
            return true;
        } catch(Exception e){
            System.out.println("Exception while deleting  node"+e);
            return false;
        }    
        
    }

    public int successor(int key){
        return this.ring.successor(key);
    }

    public int predecessor(int key){
        return this.ring.predecessor(key);
    }

    public Node successorObject(int key){
        return this.ring.successorObject(key);
    }

    public Node predecessorObject(int key){
        return this.ring.predecessorObject(key);
    }

    public Node getObject(int key){
        return this.ring.get(key);
    }

    public Node routeNode(String key){

        int hash = (int) (hashFunction.hash( key) % size);
        System.out.println("Hash of key is "+hash);
        return this.ring.successorObject(hash);
    }
    


    public List<Node> getPreferenceList(String key, int count, Node ignoreNode){

        Set<Node> nodeList = new HashSet<>();
        
        int hash = (int) (hashFunction.hash( key) % size);
        int counter = 0;
        while(counter < count){
            hash = this.ring.successor(hash);
            Node node = this.ring.getObject(hash);

            if (node == null) {
                System.out.println("Not enough nodes");
                break;
            }
            if(ignoreNode != null && node.equals(ignoreNode)){
                continue;
            }
            if (!nodeList.contains(node)){
                nodeList.add(node);
                counter++;
            }
        }
        return new ArrayList<Node>(nodeList);
        
    }

    
}
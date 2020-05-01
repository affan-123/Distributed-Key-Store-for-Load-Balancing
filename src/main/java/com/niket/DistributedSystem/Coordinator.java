package com.niket.DistributedSystem;

import java.util.concurrent.ConcurrentHashMap;
import com.niket.DistributedSystem.Node;

class Coordinator{
  
  private ConcurrentHashMap<Integer,Node> routerMap;
  private Node localNode;
  
  public Coordinator(Node node){
    this.routerMap = new ConcurrentHashMap<>();
    this.localNode = node;
  }

  public void addNode(int key, Node node){
    this.routerMap.put(key,node);
  }

  public Node getNode(int key){
    return this.routerMap.get(key);
  }

  public Node getLocaNode(){
    return this.localNode;
  }
}
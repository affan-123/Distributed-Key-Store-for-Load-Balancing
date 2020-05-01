package com.niket.DistributedSystem;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Future;

import com.niket.DistributedSystem.Node;

public class ConsistentHashingCoordinator{
  
  private Node localNode;
  private ConsistentHashing router;
  private List<Node> nodeList;
  private int serverReplicaCount;
  private ExecutionThreadPool executionThreadPool;
  private static ConsistentHashingCoordinator single_instance = null;

  public static ConsistentHashingCoordinator getInstance(){
    if(single_instance == null){
      single_instance = new ConsistentHashingCoordinator(null, 16, 0);
    }
    return single_instance;
  }

  public static ConsistentHashingCoordinator getInstance(Node node, int universeSize, int replicaCount, Collection<Node> nodeList){
    if(single_instance == null){
      
      single_instance = new ConsistentHashingCoordinator(node, universeSize, replicaCount, nodeList);
    }
    return single_instance;
  }

  public static ConsistentHashingCoordinator getInstance(Node node, int universeSize, int replicaCount){
    if(single_instance == null){

      single_instance = new ConsistentHashingCoordinator(node, universeSize, replicaCount);
    }
    return single_instance;
  }
  
  private ConsistentHashingCoordinator(Node node, int universeSize, int replicaCount){
    this.router = new ConsistentHashing(universeSize, replicaCount); 
    this.localNode = node;
    this.serverReplicaCount = replicaCount;
    this.nodeList = new LinkedList<>();
    this.executionThreadPool = ExecutionThreadPool.getInstance();
  }

  private ConsistentHashingCoordinator(Node node, int universeSize, int replicaCount, Collection<Node> nodeList){
    this.router = new ConsistentHashing(universeSize, replicaCount, nodeList);
    this.localNode = node;
    this.serverReplicaCount = replicaCount;
    this.nodeList = new LinkedList<>();
    this.executionThreadPool = ExecutionThreadPool.getInstance();
  }

  public int getUniverseSize(){
    return router.size();
  }

  public int getServerReplicaCount(){
    return this.serverReplicaCount;
  }

  public int getServerCount(){
    return this.nodeList.size();
  }

  public int getHash(String key){
    return this.router.getHash(key);
  }

  public List<Node> getNodeList(){
    return this.nodeList;
  }

  public void debug(){
      this.router.debug();
  }

  public void addNode( Node node){
    this.router.addNode(node);
    this.nodeList.add(node);
    this.debug();
  }

  public Node routeNode(String key){
    return this.router.routeNode(key);
  }

  public boolean deleteNode(Node node){
      if(this.nodeList.contains(node)){
        this.nodeList.remove(node);
      }
      return this.router.deleteNode(node);
  }

  public List<Node> getPreferenceList(String key, int count){
    return getPreferenceList(key, count, null);
}



  public List<Node> getPreferenceList(String key, int count, Node ignoreNode){
      return this.router.getPreferenceList(key, count, ignoreNode);
  }

  public int successor(int key){
    return this.router.successor(key);
}

public int predecessor(int key){
    return this.router.predecessor(key);
}

public Node successorObject(int key){
    return this.router.successorObject(key);
}

public Node predecessorObject(int key){
    return this.router.predecessorObject(key);
}

public Node getObject(int hash){
  return this.router.getObject(hash);
}
 

  public Node getLocaNode(){
    return this.localNode;
  }

  public boolean handleNodeAddition(Node node){
        //this.avialableMap.remove(node);
      KVServer kvserver = KVServer.getInstance();
      int replica = kvserver.getReplicaCount();
      //consistentHashingCoordinator.deleteNode(node);
      if(this.getServerCount() < replica){
          System.out.print("TOo less server");
          return false;
      }
      List<Future<Boolean>> resultList = new ArrayList<>();
      for(int i=0;i<this.getServerReplicaCount();i++){
          int hash = this.getHash(node.getID()+Integer.toString(i));
          Node prevNode = getObject(hash);
          List<Node> successorNodeList = this.getPreferenceList(node.getID()+Integer.toString(i), replica);
          System.out.println("Successor list is "+successorNodeList.toString());

          int prev = hash;
          int next = 0;
          boolean sameHash = false;
          if(prevNode != null){
            System.out.println("Special case");
            sameHash = true;
            successorNodeList.add(0,prevNode);
            successorNodeList.remove(successorNodeList.size()-1);
            System.out.println("Successor list is "+successorNodeList.toString());
            //prevNode = this.predecessorObject(hash);
            System.out.println("Pre node for i "+i+" is "+prevNode.toString()+" for  hash "+hash);
            System.out.println("Successor list is "+successorNodeList.toString());
            prev = hash;
            next = this.predecessor(prev);

            final int l = next;
            final int h = prev;
            prev = next;
            if (l > h){
                System.out.println("SAme HASH Abnormal case");
                Future<Boolean>  result = executionThreadPool.executor.submit(()->{
                    try{
                        System.out.println("First Shifting keys from "+0+" to "+h+" from "+ successorNodeList.get(0)+" to "+node.toString() );
                        KVClient client = new KVClient(successorNodeList.get(0));
                        client.balanceadd(0, h, this.getUniverseSize(), node);
                        System.out.println("First Shifting keys from "+l+" to "+this.getUniverseSize()+" from "+ node.toString() );
                        client.balanceadd(l, this.getUniverseSize() , this.getUniverseSize(), node);

                        client = new KVClient(successorNodeList.get(0));
                        System.out.println("First Deleting keys from "+0+" to "+h+" from "+ successorNodeList.get(0) );
                        client.deleteInRange(0,h,this.getUniverseSize());
                        System.out.println("First Deleting keys from "+0+" to "+h+" from "+ successorNodeList.get(0) );
                        client.deleteInRange(l,this.getUniverseSize() ,this.getUniverseSize());
                        return true;
                    } catch(Exception e){
                        System.out.println("Exception while sending migrate message");
                        return false;
                    }
                });
                resultList.add(result);
            } else{
                System.out.println("SAME HASH normal case");
                Future<Boolean>  result = executionThreadPool.executor.submit(()->{
                    try{
                        System.out.println("First Shifting keys from "+l+" to "+h+" from "+ successorNodeList.get(0)+" to "+node.toString() );
                        KVClient client = new KVClient(successorNodeList.get(0));
                        client.balanceadd(l, h, this.getUniverseSize(),node  );
                        client = new KVClient(successorNodeList.get(0));
                        System.out.println("First Deleting keys from "+l+" to "+h+" from "+ successorNodeList.get(0) );
                        client.deleteInRange(l,h,this.getUniverseSize());
                        return true;
                    } catch(Exception e){
                        System.out.println("Exception while sending migrate message");
                        return false;
                    }
                });
                resultList.add(result);


            }
          
          } else{
            prevNode = this.predecessorObject(hash);
            System.out.println("Pre node for i "+i+" is "+prevNode.toString()+" for  hash "+hash);
            System.out.println("Successor list is "+successorNodeList.toString());
            prev = hash;
            next = this.predecessor(prev);
            final int l = next;
            final int h = prev;
            prev = next;
            if (l > h){
                System.out.println("Abnormal case");
                Future<Boolean>  result = executionThreadPool.executor.submit(()->{
                    try{
                        System.out.println("First Shifting keys from "+0+" to "+h+" from "+ successorNodeList.get(0)+" to "+node.toString() );
                        KVClient client = new KVClient(successorNodeList.get(0));
                        client.balanceadd(0, h, this.getUniverseSize(), node);
                        System.out.println("First Shifting keys from "+l+" to "+this.getUniverseSize()+" from "+ node.toString() );
                        client.balanceadd(l, this.getUniverseSize() , this.getUniverseSize(), node);

                        client = new KVClient(successorNodeList.get(replica-1));
                        System.out.println("First Deleting keys from "+0+" to "+h+" from "+ successorNodeList.get(replica-1) );
                        client.deleteInRange(0,h,this.getUniverseSize());
                        System.out.println("First Deleting keys from "+0+" to "+h+" from "+ successorNodeList.get(replica-1) );
                        client.deleteInRange(l,this.getUniverseSize() ,this.getUniverseSize());
                        return true;
                    } catch(Exception e){
                        System.out.println("Exception while sending migrate message");
                        return false;
                    }
                });
                resultList.add(result);
            } else{
                System.out.println("normal case");
                Future<Boolean>  result = executionThreadPool.executor.submit(()->{
                    try{
                        System.out.println("First Shifting keys from "+l+" to "+h+" from "+ successorNodeList.get(0)+" to "+node.toString() );
                        KVClient client = new KVClient(successorNodeList.get(0));
                        client.balanceadd(l, h, this.getUniverseSize(),node  );
                        client = new KVClient(successorNodeList.get(replica-1));
                        System.out.println("First Deleting keys from "+l+" to "+h+" from "+ successorNodeList.get(replica-1) );
                        client.deleteInRange(l,h,this.getUniverseSize());
                        return true;
                    } catch(Exception e){
                        System.out.println("Exception while sending migrate message");
                        return false;
                    }
                });
                resultList.add(result);


            }
          }
          final boolean finalSameHash = sameHash;
          final Node finalPrevNode = prevNode;
          for(int j = 1; j < replica; j++){
              next = this.predecessor(prev);
              if(this.getObject(next).equals(this.getObject(prev))){
                System.out.println("Same object wtf ");
                j--;
                prev = next;
                continue;
              }
              final int low = next;
              final int high = prev;
              final int offset = j;
              System.out.println("workiing for j "+j+" for hash "+hash);
              if (low > high){
                  System.out.println("Abnormal case");
                  Future<Boolean>  result = executionThreadPool.executor.submit(()->{
                      try{
                          System.out.println("Shifting keys from "+0+" to "+high+" from "+ finalPrevNode+" to "+node );
                          KVClient client = new KVClient(finalPrevNode);
                          client.balanceadd(0, high, this.getUniverseSize(), node);
                          System.out.println("Shifting keys from "+low+" to "+this.getUniverseSize()+" from "+ finalPrevNode+" to "+ node );
                          client.balanceadd(low, this.getUniverseSize() , this.getUniverseSize(), node);
                          if(finalSameHash){
                            System.out.println("Same hash");
                            client = new KVClient(successorNodeList.get(0));
                            System.out.println(" Deleting keys from "+0+" to "+high+" from "+ successorNodeList.get(0) );
                            client.deleteInRange(0,high,this.getUniverseSize());
                            System.out.println(" Deleting keys from "+low+" to "+this.getUniverseSize()+" from "+ successorNodeList.get(0) );
                            client.deleteInRange(low,this.getUniverseSize(),this.getUniverseSize());
                         

                          }else{
                            client = new KVClient(successorNodeList.get(replica-1-offset));
                            System.out.println(" Deleting keys from "+0+" to "+high+" from "+ successorNodeList.get(replica-1-offset) );
                            client.deleteInRange(0,high,this.getUniverseSize());
                            System.out.println(" Deleting keys from "+low+" to "+this.getUniverseSize()+" from "+ successorNodeList.get(replica-1-offset) );
                            client.deleteInRange(low,this.getUniverseSize(),this.getUniverseSize());
                           
                          }
                           return true;
                        } catch(Exception e){
                          System.out.println("Exception while sending migrate message");
                          return false;
                      }
                  });
                  resultList.add(result);

              } else{
                  System.out.println("normal case");
                  Future<Boolean>  result = executionThreadPool.executor.submit(()->{
                      try{
                          System.out.println("Shifting keys from "+low+" to "+high+" from "+ finalPrevNode+" to "+node );
                          KVClient client = new KVClient(finalPrevNode);
                          client.balanceadd(low, high, this.getUniverseSize(), node);
                          
                          if(finalSameHash){
                            System.out.println("Same HASh");
                            client = new KVClient(successorNodeList.get(0));
                            System.out.println("Deleting keys from "+low+" to "+high+" from "+ successorNodeList.get(0) );
                            client.deleteInRange(low,high,this.getUniverseSize());
                            
                          }else{
                            client = new KVClient(successorNodeList.get(replica-1-offset));
                            System.out.println("Deleting keys from "+low+" to "+high+" from "+ successorNodeList.get(replica-1-offset) );
                            client.deleteInRange(low,high,this.getUniverseSize());
                            
                          }
                          return true;
                        } catch(Exception e){
                          System.out.println("Exception while sending migrate message");
                          return false;
                      }
                  });
                  resultList.add(result);

                  
              }
              prev = next;
              try{
                for(Future<Boolean> f : resultList){
                  if(!f.get()){
                    return false;
                  }
                }
        
              } catch(Exception e){
                System.out.println("Exception while checking adding new node result");
                return false;
              }

          }
      }

      System.out.println("length of result is "+resultList.size());
      try{
        for(Future<Boolean> f : resultList){
          if(!f.get()){
            return false;
          }
        }

      } catch(Exception e){
        System.out.println("Exception while checking adding new node result");
        return false;
      }

    return true;
      
      
      
    
}






}
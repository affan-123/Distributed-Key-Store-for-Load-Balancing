package com.niket.DistributedSystem.FailureDetection;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.swing.CellEditor;

import com.niket.DistributedSystem.ConsistentHashingCoordinator;
import com.niket.DistributedSystem.ExecutionThreadPool;
import com.niket.DistributedSystem.KVClient;
import com.niket.DistributedSystem.KVServer;
import com.niket.DistributedSystem.Node;

public class HeartBeatManager {


    private static HeartBeatManager single_instance = null;
    private Node lNode;
    private PhiAccrualFailureDetector phiAccrualFailureDetector;
    private double THRESHOLD = 10;
    private ConsistentHashingCoordinator consistentHashingCoordinator = ConsistentHashingCoordinator.getInstance();

    public static HeartBeatManager getInstance(Node lNode){
        if(single_instance == null){
            single_instance = new HeartBeatManager(lNode);
        }
        return single_instance;
    }

    private ConcurrentHashMap<Node,Boolean> avialableMap;
    private ScheduledExecutorService executorService;
    private ExecutionThreadPool executionThreadPool;

    private HeartBeatManager(Node node){
        this.avialableMap = new ConcurrentHashMap<>();
        this.executorService = Executors.newScheduledThreadPool(1);
        this.lNode = node;
        executionThreadPool = ExecutionThreadPool.getInstance();
        this.phiAccrualFailureDetector = PhiAccrualFailureDetector.getInstance();
        
    }

    public boolean addNode(Node node, Boolean status){
        try{
            System.out.println("Adding "+node.toString());
            this.avialableMap.put(node, status);
            this.phiAccrualFailureDetector.addNode(node);
            
            return true;
        } catch(Exception e){
            System.out.println("Exception while adding node to map"+ e);
            return false;
        }
    }

    public void report(Node node){
        this.phiAccrualFailureDetector.report(node);
    }

    public double phi(Node node){
        return this.phiAccrualFailureDetector.phi(node);
    }
    public boolean Status(Node node){
        return (phi(node) > this.THRESHOLD)?false:true;
    }

    public boolean removeNode(Node node){
        try{
          
            this.avialableMap.remove(node);
            return true;
        } catch(Exception e){
            System.out.println("Exception while removing node to map"+ e);
            return false;
        }
    }

    public void setAvialable(Node node){
        this.avialableMap.put(node, true);
    }

    public void setUnAvialable(Node node){
        this.avialableMap.put(node, false);
    }

    public boolean getObject(Node node){
        if(node!=null){
            System.out.print(this.avialableMap);
            return this.avialableMap.get(node);
        }
        
        else return false;
    }

    public void handleFailure(Node node){
        if(!Status(node)){
            this.avialableMap.remove(node);
            KVServer kvserver = KVServer.getInstance();
            int replica = kvserver.getReplicaCount();
            List<Future<Boolean>> resultList = new ArrayList<>();
            if(consistentHashingCoordinator.getServerCount() < replica){
                System.out.print("TOo less server");
                return;
            }
            for(int i=0;i<consistentHashingCoordinator.getServerReplicaCount();i++){
                int hash = consistentHashingCoordinator.getHash(node.getID()+Integer.toString(i));
                Node prevNode = consistentHashingCoordinator.predecessorObject(hash);
                System.out.println("Pre node for i "+i+" is "+prevNode.toString()+" for  hash "+hash);
                List<Node> successorNodeList = consistentHashingCoordinator.getPreferenceList(node.getID()+Integer.toString(i), replica, node);
                System.out.println("Successor list is "+successorNodeList.toString());
                int prev = hash;
                int next = consistentHashingCoordinator.predecessor(prev);
                
                final int l = next;
                final int h = prev;
                prev = next;

                if (l > h){
                    System.out.println("Abnormal case");
                    Future<Boolean> res = executionThreadPool.executor.submit(()->{
                        try{
                            System.out.println("First Shifting keys from "+0+" to "+h+" from "+ successorNodeList.get(0)+" to "+successorNodeList.get(successorNodeList.size()-1) );
                            KVClient client = new KVClient(successorNodeList.get(0));
                            client.balanceadd(0, h, consistentHashingCoordinator.getUniverseSize(), successorNodeList.get(successorNodeList.size()-1));
                            System.out.println("First Shifting keys from "+l+" to "+consistentHashingCoordinator.getUniverseSize()+" from "+ successorNodeList.get(0)+" to "+successorNodeList.get(successorNodeList.size()-1) );

                            client.balanceadd(l, consistentHashingCoordinator.getUniverseSize() , consistentHashingCoordinator.getUniverseSize(), successorNodeList.get(successorNodeList.size()-1));
                            return true;
                        } catch(Exception e){
                            System.out.println("Exception while sending migrate message");
                            return false;
                        }
                    });
                    resultList.add(res);


                } else{
                    System.out.println("normal case");

                    Future<Boolean> res = executionThreadPool.executor.submit(()->{
                        try{
                            System.out.println("First Shifting keys from "+l+" to "+h+" from "+ successorNodeList.get(0)+" to "+successorNodeList.get(successorNodeList.size()-1) );
                            KVClient client = new KVClient(successorNodeList.get(0));
                            client.balanceadd(l, h, consistentHashingCoordinator.getUniverseSize(), successorNodeList.get(successorNodeList.size()-1));
                            return true;
                        } catch(Exception e){
                            System.out.println("Exception while sending migrate message");
                            return false;
                        }
                    });
                    resultList.add(res);

                    
                }

                for(int j = 1; j < replica; j++){
                    next = consistentHashingCoordinator.predecessor(prev);
                    final int low = next;
                    final int high = prev;
                    System.out.println("Handling range "+low+" and "+high);
                    int tempPrev = prev;
                    while(consistentHashingCoordinator.getObject(next).equals(consistentHashingCoordinator.getObject(tempPrev))){
                        System.out.println("Same object wtf at "+prev+" and "+next);
                        tempPrev = next;
                        next = consistentHashingCoordinator.predecessor(tempPrev);
                        System.out.print("Now next is "+next);
                      }
                    final int offset = j;
                    
                    
                    System.out.println("workiing for j "+j+" for hash "+hash);
                    if (low > high){
                        System.out.println("Abnormal case");

                        Future<Boolean> res = executionThreadPool.executor.submit(()->{
                            try{
                                System.out.println("Shifting keys from "+0+" to "+high+" from "+ prevNode+" to "+successorNodeList.get(successorNodeList.size()-1-offset) );

                                KVClient client = new KVClient(prevNode);
                                client.balanceadd(0, high, consistentHashingCoordinator.getUniverseSize(), successorNodeList.get(successorNodeList.size()-offset-1));
                                System.out.println("Shifting keys from "+low+" to "+consistentHashingCoordinator.getUniverseSize()+" from "+ prevNode+" to "+successorNodeList.get(successorNodeList.size()-1-offset) );

                                client.balanceadd(low, consistentHashingCoordinator.getUniverseSize() , consistentHashingCoordinator.getUniverseSize(), successorNodeList.get(successorNodeList.size()-offset-1));
                                return true;
                            } catch(Exception e){
                                System.out.println("Exception while sending migrate message");
                                return false;
                            }
                        });
                        resultList.add(res);

                    } else{
                        System.out.println("normal case");

                        Future<Boolean> res = executionThreadPool.executor.submit(()->{
                            try{
                                System.out.println("Shifting keys from "+low+" to "+high+" from "+ prevNode+" to "+successorNodeList.get(successorNodeList.size()-1-offset) );

                                KVClient client = new KVClient(prevNode);
                                client.balanceadd(low, high, consistentHashingCoordinator.getUniverseSize(), successorNodeList.get(successorNodeList.size()-offset-1));
                                return true;
    
                            } catch(Exception e){
                                System.out.println("Exception while sending migrate message");
                                return false;
                            }
                        });
                        resultList.add(res);
                        
                    }
                    prev = next;
                    try{
                        for(Future<Boolean> f : resultList){
                          if(!f.get()){
                            return ;
                          }
                        }
                
                      } catch(Exception e){
                        System.out.println("Exception while checking adding new node result");
                        return ;
                      }

                }
            }
            try{
                consistentHashingCoordinator.deleteNode(node);

                KVClient client = new KVClient(lNode);
                client.deleteNode(node);
            } catch(Exception e){
                System.out.println("Exception while broadcasting delete Node"+e);
            }

        }
    }

    public void start(){
        this.executorService.scheduleAtFixedRate(new Runnable(){

            public void run(){
                try{
                    for(Node node : consistentHashingCoordinator.getNodeList()){
                        //System.out.println("Client Sending heartbeat to "+node.toString());
                        if(node.equals(lNode)){
                            continue;
                        }
                        HeartBeatClient client = new HeartBeatClient(node);
                        client.sendHeartBeat(lNode);
                        //System.out.println("Status of  Node "+node.toString()+" is "+avialableMap.get(node));
                        System.out.println("PHI of  Node "+node.toString()+" is "+ phi(node));
                        avialableMap.put(node, Status(node));
                        //avialableMap.put(node, result);
                        if(lNode.getPort() == 8081){
                            handleFailure(node);
                        }
                    }
                } catch(Exception e){
                    System.out.println("Exception while Sending Echo"+e);
                }
                
            }

        }, 3, 1, TimeUnit.SECONDS);
    }


    
    


}
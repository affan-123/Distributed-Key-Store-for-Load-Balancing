@ECHO 
CD C:\Users\AFFAN SHAIKH\eclipse-workspace\Distributed-KeyStore--master

 
 
FOR /L %%d IN (1 1 5 ) DO  (

start /b java -cp .\target\com.niket.DistributedKeyStore-1.0-SNAPSHOT-jar-with-dependencies.jar com.niket.DistributedSystem.Client 8081 8082 8083 >> out_a%%d.txt 
start /b java -cp .\target\com.niket.DistributedKeyStore-1.0-SNAPSHOT-jar-with-dependencies.jar com.niket.DistributedSystem.Client 8081 8082 8083 >> out_b%%d.txt 
 
)

@PAUSE

@ECHO
CD C:\Users\AFFAN SHAIKH\eclipse-workspace\Distributed-KeyStore--master 
FOR /L %%d IN (1 1 45) DO  ( 
start /b java -cp .\target\com.niket.DistributedKeyStore-1.0-SNAPSHOT-jar-with-dependencies.jar com.niket.DistributedSystem.ClientA 8081 8082 8083 8084 8085 8086 > ./test_results/out_a%%d.txt 
start /b java -cp .\target\com.niket.DistributedKeyStore-1.0-SNAPSHOT-jar-with-dependencies.jar com.niket.DistributedSystem.ClientB 8081 8082 8083 8084 8085 8086 > ./test_results/out_b%%d.txt 
start /b java -cp .\target\com.niket.DistributedKeyStore-1.0-SNAPSHOT-jar-with-dependencies.jar com.niket.DistributedSystem.ClientC 8081 8082 8083 8084 8085 8086 > ./test_results/out_c%%d.txt 
)

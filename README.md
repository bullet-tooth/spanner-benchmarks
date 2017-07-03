# spanner-benchmarks

it uses [JMH](http://openjdk.java.net/projects/code-tools/jmh/) for benchmarking.  
For running benckmark you should do next:  
Build project `mvn package`  
Run by command `java -Dspanner.sessions.min=100 -Dspanner.sessions.write.fraction=0f -Dspanner.channels.num=16 -Dspanner.clientthreads.num=50 -jar target/benchmarks.jar BenchmarkSpanner.blindWrite -t 50 -i 10 -wi 3                   
`  

Where, Spanner related:  
`-Dspanner.sessions.min` minimum sessions in session pool. They will be created immediately. Default: 100   
`-Dspanner.sessions.write.fraction` Fraction of sessions to be kept prepared for write transactions. Default: 0  
`-Dspanner.channels.num` The number of gRPC channels to use(physical connection). Default: 16 
`-Dspanner.clientthreads.num` Client threads for executor service. Default: 100  
 JMH related:  
`BenchmarkSpanner.blindWrite` Benchmark to execute. All if not specified  
`-t` Thread count  
`-i` Iterations  
`-wi` Warm up iterations  
All arguments you can find at help `java -jar target/benchmarks.jar -h`

 

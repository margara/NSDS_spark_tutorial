# Apache Spark tutorial

Apache Spark tutorial for the NSDS course in Politecnico di Milano

## Execution

- All examples can run with default parameters (defined in commons.Consts), or accept arguments from command line to override them.
- To run in local mode, keep the default values
- To run in cluster mode, set the master URL to the Spark master URL, e.g. `spark://localhost:7077`

## Trubleshooting

- If Spark requires to access the internals of Java modules (for Java >= 9), 
add the following JVM option: 

        --add-opens java.base/java.nio=ALL-UNNAMED \
        --add-opens java.base/java.lang.invoke=ALL-UNNAMED \
        --add-opens java.base/java.util=ALL-UNNAMED
(defproject com.leadtune/apparatus "2.0.0"
  :description "Apparatus: Clojure Clusters"
  :dependencies [[org.clojure/clojure "1.3.0"]
                 [com.hazelcast/hazelcast "1.9.4.6"]
                 [commons-daemon "1.0.5"]]
  :aot [apparatus.eval apparatus.remote-function]
  :main apparatus.main
  :dev-dependencies [[swank-clojure "1.4.0-SNAPSHOT"]
                     [log4j/log4j "1.2.13"]]
  :jvm-opts ["-Dhazelcast.logging.type=log4j"])

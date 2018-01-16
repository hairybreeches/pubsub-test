(defproject pubsub-test "__VERSION__"
  :description "pubsub harness for reducing duplicate messages"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [cheshire "5.7.0"]
                 [medley "0.8.4"]
                 [com.google.cloud/google-cloud-pubsub "0.32.0-beta"]
                 ;; logging
                 [com.taoensso/timbre "4.8.0"]
                 [com.fzakaria/slf4j-timbre "0.3.4"]
                 [log4j/log4j "1.2.17"]
                 [org.slf4j/jul-to-slf4j "1.7.25"]
                 [org.slf4j/jcl-over-slf4j "1.7.25"]

]

  :plugins [[info.sunng/lein-bootclasspath-deps "0.3.0"]]
  :boot-dependencies [[org.mortbay.jetty.alpn/alpn-boot "8.1.11.v20170118"]]
 )


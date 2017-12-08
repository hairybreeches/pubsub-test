(defproject pubsub-test "__VERSION__"
  :description "meters-workers for entity-store"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [cheshire "5.7.0"]
                 [medley "0.8.4"]
                 [com.google.cloud/google-cloud "0.30.0-alpha"]
                 [com.google.guava/guava "23.4-jre"]]

  :profiles {:dev {:source-paths ["dev" "src" "it"]
                   :test-paths ["test"]
                   :resource-paths ["test/resources"]
                   :dependencies [[rest-cljer "0.2.1"]
                                  [ring/ring-mock "0.3.0"]
                                  [org.apache.curator/curator-client "3.3.0"]
                                  [org.apache.curator/curator-test "3.3.0"]
                                  [org.apache.kafka/kafka_2.11 "0.10.0.0"]
                                  [integrant/repl "0.2.0"]]
                   :env {:restdriver-port "8081"}}
             :integration {:test-paths ["it"]}
             :uberjar {}}

    :plugins [[lein-environ "0.5.0"]
            [venantius/ultra "0.5.1"]
            [jonase/eastwood "0.2.3"]]

  :eastwood {:exclude-linters [:def-in-def :unused-ret-vals]}

  :test-selectors {:default (complement #(or (:integration %)
                                           (:acceptance %)
                                           (:schema-validation %)))
                   :acceptance :acceptance
                   :integration :integration
                   :schema-validation :schema-validation
                   :all (constantly true)}
  :jvm-opts ["-XX:MaxJavaStackTraceDepth=-1"])


(defproject com.rpl/rama-tpcc "1.0.0-SNAPSHOT"
  :source-paths ["src/clj"]
  :java-source-paths ["src/java"]
  :dependencies [[org.clojure/clojure "1.12.4"]
                 [com.github.f4b6a3/uuid-creator "6.1.1"]
                 [com.rpl/rama-helpers "0.10.0" :exclusions [org.clojure/clojure]]]
  :repositories [["releases" {:id "maven-releases"
                              :url "https://nexus.redplanetlabs.com/repository/maven-public-releases"}]]

  :global-vars {*warn-on-reflection* true}

  :profiles {:dev {:resource-paths ["test/resources/"]}
             :provided {:dependencies [[com.rpl/rama "1.5.0"]]}}
  )

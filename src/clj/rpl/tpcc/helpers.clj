(ns rpl.tpcc.helpers
  (:import
    [com.github.f4b6a3.uuid UuidCreator]
    [com.rpl.rama.helpers TopologyUtils]
    [java.util.concurrent ThreadLocalRandom]))

(def DAY-MILLIS (* 1000 60 60 24))

(defn sortv [coll]
  (vec (sort coll)))

(defn make-request-id
  "Create a request ID: high bits = timestamp millis, low bits = random long."
  ^java.util.UUID []
  (java.util.UUID. (TopologyUtils/currentTimeMillis) (.nextLong (ThreadLocalRandom/current))))

(defn request-id-day-bucket
  "Extract the day bucket (days since epoch) from a request ID."
  ^long [^java.util.UUID id]
  (quot (.getMostSignificantBits id) DAY-MILLIS))

;; TPC-C spec 4.3.2.2: character set must represent minimum 128 different characters,
;; including a-z, A-Z, 0-9. We use printable ASCII 33-126 (94 chars, excluding space)
;; plus chars 161-194 to reach 128 total.
(def ^:private ^String astring-chars
  (let [sb (StringBuilder. 128)]
    (dotimes [i 94] (.append sb (char (+ 33 i))))    ;; ! through ~
    (dotimes [i 34] (.append sb (char (+ 161 i))))   ;; ¡ through Â
    (.toString sb)))
(def ^:private alpha-only "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz")

(defn fast-rand-int
  "Like rand-int but uses ThreadLocalRandom (no global lock)."
  ^long [^long n]
  (.nextInt (ThreadLocalRandom/current) (int n)))

(defn fast-rand
  "Like rand but uses ThreadLocalRandom (no global lock)."
  ^double []
  (.nextDouble (ThreadLocalRandom/current)))

(defn rand-astring
  "Random a-string per TPC-C spec 4.3.2.2: 128-char set including a-z, A-Z, 0-9."
  [min-len max-len]
  (let [^ThreadLocalRandom rng (ThreadLocalRandom/current)
        ^String an astring-chars
        len (+ (int min-len) (.nextInt rng (inc (- (int max-len) (int min-len)))))
        sb (StringBuilder. len)]
    (dotimes [_ len]
      (.append sb (.charAt an (.nextInt rng (.length an)))))
    (.toString sb)))

(defn rand-letter-string
  "Random alphabetic string (letters only) with length in [min-len, max-len]."
  [min-len max-len]
  (let [^ThreadLocalRandom rng (ThreadLocalRandom/current)
        ^String al alpha-only
        len (+ (int min-len) (.nextInt rng (inc (- (int max-len) (int min-len)))))
        sb (StringBuilder. len)]
    (dotimes [_ len]
      (.append sb (.charAt al (.nextInt rng 52))))
    (.toString sb)))

(defn rand-nstring
  "Random numeric string with length in [min-len, max-len]."
  [min-len max-len]
  (let [^ThreadLocalRandom rng (ThreadLocalRandom/current)
        len (+ (int min-len) (.nextInt rng (inc (- (int max-len) (int min-len)))))
        sb (StringBuilder. len)]
    (dotimes [_ len]
      (.append sb (char (+ 48 (.nextInt rng 10)))))
    (.toString sb)))

(defn rand-zip
  "TPC-C zip code: random 4-digit nstring + '11111'."
  []
  (let [^ThreadLocalRandom rng (ThreadLocalRandom/current)
        sb (StringBuilder. 9)]
    (dotimes [_ 4]
      (.append sb (char (+ 48 (.nextInt rng 10)))))
    (.append sb "11111")
    (.toString sb)))

(defn warehouse-partition
  "Deterministic warehouse-to-task mapping: (mod (dec w-id) num-partitions).
   Gives perfect balance for sequential warehouse IDs.
   Arg order matches |custom convention (num-partitions first) when arity-2."
  (^long [^long num-partitions ^long w-id]
   (mod (dec w-id) num-partitions)))

(defn nurand
  "NURand(A, x, y) per TPC-C spec: (((random(0,A) | random(x,y)) + C) % (y-x+1)) + x"
  [a x y c]
  (let [^ThreadLocalRandom rng (ThreadLocalRandom/current)
        ra (.nextInt rng (inc (int a)))
        rxy (+ (int x) (.nextInt rng (inc (- (int y) (int x)))))]
    (+ (mod (+ (bit-or ra rxy) (int c)) (inc (- (int y) (int x))))
       (int x))))

(defn map-put!
  [^java.util.Map m k v]
  (.put m k v))

(defn random-uuid7
  []
  (UuidCreator/getTimeOrderedEpoch))

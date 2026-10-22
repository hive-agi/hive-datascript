(ns hive-datascript.swarm.contracts-test
  "Malli contract facets for the read port (queries), the connection
   lifecycle (connection) and coordinator registration (coordinator).

   Every test in this ns runs with those namespaces INSTRUMENTED: inputs and
   outputs are validated against their `m/=>` schemas and a violation throws,
   so each scenario/facet below doubles as a contract-conformance check.
   Fresh in-memory conn per test via connection/*test-conn*."
  (:require [clojure.test :refer [deftest testing is use-fixtures]]
            [clojure.string]
            [clojure.test.check.generators :as gen]
            [malli.core :as m]
            [malli.instrument :as mi]
            [hive-datascript.swarm.connection :as conn]
            [hive-datascript.swarm.lings :as lings]
            [hive-datascript.swarm.queries :as queries]
            [hive-datascript.swarm.coordination.coordinator :as coord]
            [hive-test.trifecta :refer [deftrifecta]]))

(def contracted-nss
  '[hive-datascript.swarm.connection
    hive-datascript.swarm.queries
    hive-datascript.swarm.coordination.coordinator])

(defn- instrument-fixture [f]
  (mi/instrument! {:filters [(apply mi/-filter-ns contracted-nss)]
                   :scope #{:input :output}})
  (try (f) (finally (mi/unstrument! {:filters [(apply mi/-filter-ns contracted-nss)]}))))

(defn- fresh-conn-fixture [f]
  (conn/with-test-conn (conn/create-conn) f))

(use-fixtures :once instrument-fixture)
(use-fixtures :each fresh-conn-fixture)

;;; =============================================================================
;;; Coverage gate: every public fn of the contracted nss carries an m/=>
;;; =============================================================================

(deftest contracted-namespaces-fully-covered-test
  (let [fs (m/function-schemas)]
    (doseq [n contracted-nss
            [sym v] (ns-publics n)
            :when (and (fn? @v) (not (:macro (meta v))))]
      (is (some? (get-in fs [n sym])) (str n "/" sym " has no m/=> contract")))))

(deftest instrumentation-rejects-bad-input-test
  (testing "instrumented subjects throw on schema-violating input"
    (is (thrown? Exception (queries/get-slave 42)))
    (is (thrown? Exception (queries/has-conflict? :not-a-path "s")))
    (is (thrown? Exception (coord/get-coordinators-by-status :zombie)))
    (is (thrown? Exception (conn/gen-id 7)))))

;;; =============================================================================
;;; Scenario: every contracted fn exercised under instrumentation
;;; =============================================================================

(deftest queries-scenario-conforms-test
  (lings/add-slave! "s-1" {:status :idle :project-id "pA" :kanban-task-id "kb-1"})
  (lings/add-slave! "s-2" {:status :working :project-id "pB" :parent "s-1"})
  (lings/add-task! "t-1" "s-1" {:status :dispatched})
  (lings/add-task! "t-2" "s-1" {:status :dispatched})
  (lings/complete-task! "t-2")
  (lings/claim-file! "a.clj" "s-1" {:task-id "t-1" :prior-hash "deadbeef"
                                    :qn "a/f" :mode :body})
  (lings/archive-claim-to-history! "a.clj" {:slave-id "s-1" :prior-hash "p"
                                            :released-hash "r"
                                            :lines-added 1 :lines-removed 2})
  (is (= "s-1" (:slave/id (queries/get-slave "s-1"))))
  (is (= "s-1" (:slave/parent (queries/get-slave "s-2"))))
  (is (some? (queries/get-slave-by-name "s-1")))
  (is (some? (queries/get-slave-by-name-or-id "agent:s-1")))
  (is (= 2 (count (queries/get-all-slaves))))
  (is (= 2 (count (queries/get-all-slaves :include-stale? true))))
  (is (= 2 (count (queries/get-all-slaves {:include-stale? true}))))
  (is (= ["s-2"] (mapv :slave/id (queries/get-slaves-by-status :working))))
  (is (= ["s-1"] (mapv :slave/id (queries/get-slaves-by-project "pA"))))
  (is (= #{"pB"} (queries/get-child-project-ids "pA")))
  (is (= ["s-1"] (vec (queries/get-slave-ids-by-project "pA"))))
  (is (= "s-1" (:slave/id (queries/get-slave-by-kanban-task "kb-1"))))
  (is (= "s-1" (:task/slave (queries/get-task "t-1"))))
  (is (= #{"t-1" "t-2"} (set (map :task/id (queries/get-tasks-for-slave "s-1")))))
  (is (= ["t-1"] (mapv :task/id (queries/get-tasks-for-slave "s-1" :dispatched))))
  (is (= ["t-2"] (mapv :task/id (queries/get-completed-tasks :slave-id "s-1"))))
  (is (= "deadbeef" (:prior-hash (queries/get-claims-for-file "a.clj"))))
  ;; only get-all-claims projects the span :qn/:mode (get-claims-for-file does not)
  (is (= [["a.clj" :body]] (mapv (juxt :file :mode) (queries/get-all-claims))))
  (is (= [{:file "a.clj" :held-by "s-1"}]
         (vec (queries/check-file-conflicts "s-2" #{"a.clj" "b.clj"}))))
  (is (= [2] (mapv :lines-removed (queries/get-recent-claim-history :file "a.clj"))))
  (is (= 2 (:slaves (queries/db-stats))))
  (is (= 2 (count (:slaves (queries/dump-db))))))

(deftest coordinator-scenario-conforms-test
  (is (some? (coord/register-coordinator! "c-1" {:project "p1"})))
  (is (some? (coord/register-coordinator! "c-2" {:project "p2" :pid 42 :session-id "s"})))
  (is (= :active (:coordinator/status (coord/get-coordinator "c-1"))))
  (is (nil? (coord/get-coordinator "ghost")))
  (is (nil? (coord/update-heartbeat! "ghost")))
  (is (some? (coord/mark-coordinator-stale! "c-1")))
  (is (= ["c-1"] (mapv :coordinator/id (coord/get-coordinators-by-status :stale))))
  (is (some? (coord/update-heartbeat! "c-1")))
  (is (= :active (:coordinator/status (coord/get-coordinator "c-1"))))
  (is (some? (coord/mark-coordinator-terminated! "c-2")))
  (is (= ["c-2"] (mapv :coordinator/id (coord/get-coordinators-for-project "p2"))))
  (is (= 2 (count (coord/get-all-coordinators))))
  (is (some? (coord/remove-coordinator! "c-2")))
  (is (nil? (coord/remove-coordinator! "c-2")))
  (is (= ["c-1"] (mapv :coordinator/id (coord/get-all-coordinators)))))

(deftest connection-scenario-conforms-test
  (let [bound (conn/current-test-conn)]
    (is (identical? bound (conn/get-conn)))
    (is (identical? bound (conn/ensure-conn)))
    (is (inst? (conn/now)))
    (is (re-matches #"task-[0-9a-f-]{36}" (conn/gen-id "task")))
    (is (= :ran (conn/with-test-conn (conn/create-conn) (constantly :ran))))
    (conn/reset-conn!)
    (is (identical? bound (conn/get-conn)))))

;;; =============================================================================
;;; Trifecta facets (golden + property + mutation), instrumented subjects
;;; =============================================================================

(defn conflict-observation
  "Claim f.clj for holder, then ask whether requester conflicts."
  [[holder requester]]
  (conn/with-test-conn (conn/create-conn)
    (fn []
      (lings/add-slave! holder {})
      (lings/claim-file! "f.clj" holder)
      (queries/has-conflict? "f.clj" requester))))

(deftrifecta has-conflict-trifecta conflict-observation
  {:golden-path "test/golden/has-conflict.edn"
   :cases {:same ["s-1" "s-1"] :other ["s-1" "s-2"]}
   :gen (gen/tuple (gen/elements ["a" "b" "c"]) (gen/elements ["a" "b" "c"]))
   :pred #(and (boolean? (:conflict? %))
               (or (not (:conflict? %)) (string? (:held-by %))))
   :num-tests 30
   :mutations [["never-conflicts" (fn [_] {:conflict? false})]]})

(defn name-or-id-observation
  "Resolve an identifier against a single seeded slave; return its id."
  [identifier]
  (conn/with-test-conn (conn/create-conn)
    (fn []
      (lings/add-slave! "swarm-fix-wave-1770230187" {:name "fix-wave"})
      (:slave/id (queries/get-slave-by-name-or-id identifier)))))

(deftrifecta name-or-id-trifecta name-or-id-observation
  {:golden-path "test/golden/name-or-id.edn"
   :cases {:exact "swarm-fix-wave-1770230187"
           :agent-prefix "agent:swarm-fix-wave-1770230187"
           :fuzzy "ling-fix-wave"
           :miss "zzz-qqq"}
   :gen (gen/fmap #(apply str %) (gen/vector gen/char-alpha 1 12))
   :pred #(or (nil? %) (= "swarm-fix-wave-1770230187" %))
   :num-tests 30
   :mutations [["always-nil" (fn [_] nil)]]})

(defn coordinator-status-observation
  "Drive a coordinator through a lifecycle op; return its status."
  [op]
  (conn/with-test-conn (conn/create-conn)
    (fn []
      (coord/register-coordinator! "c" {:project "p"})
      (case op
        :stale (coord/mark-coordinator-stale! "c")
        :terminated (coord/mark-coordinator-terminated! "c")
        :heartbeat (do (coord/mark-coordinator-stale! "c")
                       (coord/update-heartbeat! "c"))
        nil)
      (:coordinator/status (coord/get-coordinator "c")))))

(deftrifecta coordinator-status-trifecta coordinator-status-observation
  {:golden-path "test/golden/coordinator-status.edn"
   :cases {:none :none :stale :stale :terminated :terminated :heartbeat :heartbeat}
   :gen (gen/elements [:none :stale :terminated :heartbeat])
   :pred #{:active :stale :terminated}
   :num-tests 30
   :mutations [["always-active" (fn [_] :active)]]})

(defn gen-id-observation
  "Strip the uuid part of a generated id, leaving the prefix shape."
  [prefix]
  (let [id (if prefix (conn/gen-id prefix) (conn/gen-id))]
    (clojure.string/replace id #"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$" "<uuid>")))

(deftrifecta gen-id-trifecta gen-id-observation
  {:golden-path "test/golden/gen-id.edn"
   :cases {:none nil :task "task" :history "claim-history"}
   :gen (gen/fmap #(apply str %) (gen/vector gen/char-alpha 1 8))
   :pred #(clojure.string/ends-with? % "<uuid>")
   :num-tests 30
   :mutations [["drops-uuid" (fn [p] (str p "-"))]]})

(defn stats-observation
  "Seed n slaves each with one dispatched task; return db-stats counts."
  [n]
  (conn/with-test-conn (conn/create-conn)
    (fn []
      (dotimes [i n]
        (lings/add-slave! (str "s" i) {})
        (lings/add-task! (str "t" i) (str "s" i) {:status :dispatched}))
      (select-keys (queries/db-stats) [:slaves :tasks :active-tasks]))))

(deftrifecta db-stats-trifecta stats-observation
  {:golden-path "test/golden/db-stats.edn"
   :cases {:zero 0 :three 3}
   :gen (gen/choose 0 5)
   :pred #(= (:slaves %) (:tasks %) (:active-tasks %))
   :num-tests 20
   :mutations [["miscounts" (fn [n] {:slaves n :tasks n :active-tasks 0})]]})

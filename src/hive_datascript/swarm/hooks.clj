(ns hive-datascript.swarm.hooks
  "Host-installed hooks the swarm store calls on write paths.

   The store never names the orchestration layer. The host installs:
   - :ledger-append   (fn [event]) durable write-through for terminal events
   - :claim-released  (fn [file-path]) mirror a released claim elsewhere
     (e.g. the core.logic claim db)
   - :stale-threshold-ms  activity window for hiding stale ling rows
   Absent hooks are no-ops; an absent threshold falls back to 30 minutes."
  (:require [taoensso.timbre :as log]))

;; SPDX-License-Identifier: MIT

(def default-stale-threshold-ms
  "Activity-staleness threshold used when the host installs none (30 minutes)."
  (* 30 60 1000))

(defonce ^:private hooks (atom {}))

(defn install!
  "Merge HOOK-MAP into the installed hooks. Returns the installed map."
  [hook-map]
  (swap! hooks merge hook-map))

(defn clear!
  "Remove every installed hook. For tests."
  []
  (reset! hooks {}))

(defn installed
  "The installed hook map."
  []
  @hooks)

(defn append!
  "Ledger write-through for EVENT via the :ledger-append hook, or nil.
   Never throws."
  [event]
  (when-let [f (:ledger-append @hooks)]
    (try (f event)
         (catch Throwable t
           (log/warn "swarm ledger hook failed:" (.getMessage t))
           nil))))

(defn claim-released!
  "Tell the :claim-released hook that FILE-PATH's claim was released.
   Never throws."
  [file-path]
  (when-let [f (:claim-released @hooks)]
    (try (f file-path)
         (catch Throwable t
           (log/warn "claim-released hook failed:" (.getMessage t))
           nil))))

(defn stale-threshold-ms
  "Activity window (ms) after which a ling row counts as stale."
  []
  (or (:stale-threshold-ms @hooks) default-stale-threshold-ms))

(defn scope-rows
  "The ROWS a session owns, via the :scope-rows hook (fn [opts session-key
   rows]). OPTS carries :session-ref and :parent-of; SESSION-KEY names the
   row attribute holding the writing session. Without the hook, or without a
   :session-ref, the rows pass through unscoped."
  [opts session-key rows]
  (if-let [f (:scope-rows @hooks)]
    (f opts session-key rows)
    rows))

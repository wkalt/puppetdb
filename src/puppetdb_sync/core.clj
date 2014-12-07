(ns puppetdb-sync.core)

(def ns-prefix "puppetdb-sync.")

(defn run-command
  "Does the real work of invoking a command by attempting to result it and
   passing in args. `success-fn` is a no-arg function that is called when the
   command successfully executes.  `fail-fn` is called when a bad command is given
   or a failure executing a command."
  [success-fn fail-fn args]
  (let [subcommand (first args)]

    (let [module (str ns-prefix subcommand)
          args   (rest args)]
      (try
        (require (symbol module))
        (apply (resolve (symbol module "-main")) args)
        (success-fn)
        (catch Throwable e
          (println e)
          (fail-fn))))))

(defn -main
  [& args]
  (run-command #(System/exit 0) #(System/exit 1) args))

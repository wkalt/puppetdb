(ns puppetlabs.classifier.core
  (:gen-class))

(defn -main
  "I don't do a whole lot."
  [& args]
  (require 'puppetlabs.classifier.application)
  (apply (resolve 'puppetlabs.classifier.application/run) args))

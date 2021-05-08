(asdf:defsystem #:vellum-csv-tests
  :name "vellum-csv-tests"
  :version "0.0.0"
  :license "BSD simplified"
  :author "Marek Kochanowicz"
  :depends-on (:prove :vellum-csv)
  :defsystem-depends-on (:prove-asdf)
  :serial T
  :pathname "src"
  :components ((:test-file "tests")))

(cl:in-package #:cl-user)


(asdf:defsystem vellum-csv
  :name "vellum-csv"
  :version "0.0.0"
  :license "BSD simplified"
  :author "Marek Kochanowicz"
  :depends-on ( :iterate
                :serapeum
                :vellum
                :alexandria
                :documentation-utils-extensions
                :fare-csv)
  :serial T
  :pathname "src"
  :components ((:file "package")
               (:file "generics")
               (:file "types")
               (:file "implementation")))

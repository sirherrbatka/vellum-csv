(cl:in-package #:cl-user)


(asdf:defsystem vellum-csv
  :name "vellum-csv"
  :description "CSV support for Vellum Data Frames"
  :version "1.0.0"
  :license "BSD simplified"
  :author "Marek Kochanowicz"
  :depends-on ( #:iterate
                #:serapeum
                #:uiop
                (:version #:vellum ((>= "1.2.0")))
                #:parse-float
                #:alexandria
                #:documentation-utils-extensions)
  :serial T
  :pathname "src"
  :components ((:file "package")
               (:file "variables")
               (:file "csv")
               (:file "generics")
               (:file "types")
               (:file "functions")
               (:file "implementation")))

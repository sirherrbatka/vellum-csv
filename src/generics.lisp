(cl:in-package #:vellum.csv)


(defgeneric from-string (data-type string))
(defgeneric to-string (data-type value))

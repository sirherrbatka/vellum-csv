(cl:in-package #:vellum-csv)


(defgeneric from-string (data-type string start end))
(defgeneric to-string (data-type value))

(cl:in-package #:vellum-csv)


(defun csv-range (path &key includes-header-p header separator quote escape null-strings)
  (check-type path (or string pathname cl-ds.fs:command))
  (check-type header vellum.header:standard-header)
  (check-type separator character)
  (check-type escape character)
  (check-type quote character)
  (make-instance
   'csv-range :path path
              :header header
              :null-strings null-strings
              :escape escape
              :quote quote
              :separator separator
              :includes-header-p includes-header-p))


(defun place-null (&optional condition)
  (declare (ignore condition))
  (invoke-restart 'place-null))


(defun place-value (value)
  (invoke-restart 'place-value value))

(cl:in-package #:vellum-csv)


(defmethod vellum:to-table ((object csv-range)
                            &key
                              (key #'identity)
                              (class 'vellum.table:standard-table)
                              (header-class 'vellum.header:standard-header)
                              (columns '())
                              (body nil)
                              (header (apply #'vellum.header:make-header
                                             header-class columns)))
  (vellum:with-header (header)
    (let* ((column-count (vellum.header:column-count header))
           (function (if (null body)
                         (constantly nil)
                         (vellum:bind-row-closure body)))
           (columns (make-array column-count)))
      (iterate
        (for i from 0 below column-count)
        (setf (aref columns i)
              (vellum.column:make-sparse-material-column
               :element-type (vellum.header:column-type header i))))
      (let* ((iterator (vellum.column:make-iterator columns))
             (vellum.header:*row* (serapeum:box (make 'vellum.table:setfable-table-row
                                                      :iterator iterator)))
             (table (make class :header header :columns columns))
             (transformation (vellum.table:transformation table nil
                                                          :in-place t)))
        (cl-ds:traverse object
                        (lambda (string)
                          (let ((content (~> string make-string-input-stream
                                             fare-csv:read-csv-line)))
                            (vellum:transform-row
                             transformation
                             (lambda ()
                               (iterate
                                 (for i from 0 below column-count)
                                 (for data-type = (vellum.header:column-type header i))
                                 (for c in content)
                                 (for string = (funcall key c))
                                 (setf (vellum:rr i)
                                       (from-string data-type string))
                                 (finally (funcall function))))))))
        (vellum:transformation-result transformation)))))


(defmethod vellum:copy-from ((format (eql :csv))
                             path/range
                             &rest options)
  (~> path/range csv-range (apply #'vellum:to-table options)))


(defmethod from-string :around (type string)
  (if (string= "NULL" string)
      :null
      (parse-integer string)))


(defmethod from-string ((type (eql 'integer)) string)
  (parse-integer string))


(defmethod from-string ((type (eql 'string)) string)
  string)


(defmethod from-string ((type (eql 'float)) string)
  (parse-float string :type 'double-float))


(defmethod from-string ((type (eql 'single-float)) string)
  (coerce (parse-float string :type 'double-float)
          'single-float))


(defmethod from-string ((type (eql 'double-float)) string)
  (parse-float string :type 'double-float))


(defmethod from-string ((type (eql 'boolean)) string)
  (switch (string :test string=)
    ("1" t)
    ("0" nil)
    ("T" t)
    ("NIL" nil)
    ("True" t)
    ("False" nil)
    ("F" nil)
    ("Yes" t)
    ("No" nil)))

(cl:in-package #:vellum-csv)


(defmethod vellum:copy-to ((format (eql :csv))
                           (output stream)
                           input
                           &rest options
                           &key (includes-header-p t))
  (declare (ignore options))
  (let ((column-count (vellum:column-count input)))
    (when includes-header-p
      (iterate
        (for i from 0 below column-count)
        (collect (vellum:column-name input i)
          into fields)
        (finally (fare-csv:write-csv-line fields
                                          output))))
    (vellum:transform input
                      (vellum:bind-row ()
                        (iterate
                          (for i from 0 below column-count)
                          (for data-type =
                               (vellum:column-type input i))
                          (collect (~>> i vellum:rr
                                        (to-string data-type))
                            into fields)
                          (finally
                           (fare-csv:write-csv-line fields
                                                    output))))
                      :in-place t)
    input))


(defmethod vellum:copy-to ((format (eql :csv))
                           output
                           input
                           &rest options
                           &key (includes-header-p t))
  (declare (ignore options))
  (with-output-to-file (stream output)
    (vellum:copy-to format stream input
                    :includes-header-p includes-header-p)))


(defmethod vellum:copy-from ((format (eql :csv))
                             path/range
                             &rest options
                             &key
                             (includes-header-p t)
                             (class 'vellum.table:standard-table)
                             (header-class 'vellum.header:standard-header)
                             (columns '())
                             (body nil)
                             (header (apply #'vellum.header:make-header
                                            header-class columns)))
  (declare (ignore options))
  (~> (csv-range path/range
                 :includes-header-p includes-header-p
                 :header header)
      (vellum:to-table _
                       :body body
                       :class class)))


(defmethod from-string :around (type string)
  (if (string= "NULL" string)
      :null
      (call-next-method)))


(defmethod from-string ((type (eql 'number)) string)
  (parse-number string))


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


(defmethod from-string ((type (eql 't)) string)
  string)


(defmethod to-string (type value)
  (princ value nil))


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


(defmethod vellum.header:make-row ((header vellum.header:standard-header)
                                   (range csv-range)
                                   string)
  (iterate
   (with data = (~> string make-string-input-stream
                    fare-csv:read-csv-line))
   (with result = (~> data length make-array))
   (for elt in data)
   (for i from 0)
   (for data-type = (vellum.header:column-type header i))
   (for value = (from-string data-type elt))
   (vellum.header:check-predicate header i value)
   (setf (aref result i) value)
   (finally (return result))))

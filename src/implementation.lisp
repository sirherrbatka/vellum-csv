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
        (finally (write-csv-line fields output))))
    (vellum:transform input
                      (lambda (&rest all) (declare (ignore all))
                        (iterate
                          (for i from 0 below column-count)
                          (for data-type =
                               (vellum:column-type input i))
                          (collect (~>> i vellum:rr
                                        (to-string data-type))
                            into fields)
                          (finally
                           (write-csv-line fields output))))
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
                    :includes-header-p includes-header-p))
  input)


(defmethod vellum:copy-from ((format (eql :csv))
                             path/range
                             &rest options
                             &key
                             (includes-header-p t)
                             (class 'vellum.table:standard-table)
                             (columns '())
                             (body nil)
                             (separator #\,)
                             (quote #\")
                             (header (apply #'vellum.header:make-header columns)))
  (declare (ignore options))
  (~> (csv-range path/range
                 :includes-header-p includes-header-p
                 :separator separator
                 :quote quote
                 :header header)
      (vellum:to-table :body body
                       :class class)))


(defmethod to-string :around (type value)
  (if (eq :null value)
      ""
      (call-next-method)))


(defmethod from-string :around (type string)
  (if (or (emptyp string) (string= "NULL" string))
      :null
      (call-next-method)))


(defmethod from-string ((type (eql 'number)) string)
  (parse-number string :float-format 'double-float))


(defmethod from-string ((type (eql 'fixnum)) string)
  (parse-integer string))


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


(defmethod from-string ((type (eql t)) string)
  string)


(defmethod to-string (type value)
  (with-output-to-string (stream)
    (princ value stream)))


(defmethod to-string ((type (eql 'boolean)) value)
  (if value "True" "False"))


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


(defmethod cl-ds:traverse ((object csv-range) function)
  (with-open-file (stream (path object))
    (when (includes-header-p object)
      (read-line stream))
    (let ((*separator* (separator object))
          (*quote* (csv-quote object)))
      (validate-csv-parameters)
      (iterate
        (with buffered-stream = (make-buffered-stream :stream stream))
        (with header = (vellum.header:read-header object))
        (while (buffered-stream-peek buffered-stream))
        (for result = (read-csv-line buffered-stream))
        (iterate
          (for i from 0 below (length result))
          (for data-type = (vellum.header:column-type header i))
          (for elt = (aref result i))
          (let ((value nil))
            (tagbody main
               (setf value (from-string data-type elt))
               (unless (or (eq :null value)
                           (typep value data-type))
                 (error 'vellum.column:column-type-error
                        :expected-type data-type
                        :column i
                        :datum value)))
            (setf (aref result i) value))
          (finally (funcall function result))))))
  object)


(defmethod cl-ds:across ((object csv-range) function)
  (cl-ds:traverse object function))

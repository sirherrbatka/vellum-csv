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
                      (lambda ()
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
                             (separator #\,)
                             (header (apply #'vellum.header:make-header
                                            header-class columns)))
  (declare (ignore options))
  (~> (csv-range path/range
                 :includes-header-p includes-header-p
                 :separator separator
                 :header header)
      (vellum:to-table _
                       :body body
                       :class class)))


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


(defmethod vellum.header:make-row ((range csv-range)
                                   string)
  (iterate
    (with header = (vellum.header:read-header range))
    (with data = (let ((fare-csv:*separator* (separator range)))
                   (~> string
                       make-string-input-stream
                       fare-csv:read-csv-line)))
    (with result = (~> data length make-array))
    (for elt in data)
    (for i from 0)
    (for data-type = (vellum.header:column-type header i))
    (for value = (from-string data-type elt))
    (tagbody main
       (restart-case (vellum.header:check-predicate header i value)
         (set-to-null ()
           :report "Set the row position to :null."
           (setf value :null))
         (provide-new-value (v)
           :report "Enter the new value."
           :interactive vellum.header:read-new-value
           (setf value v)
           (go main))))
    (setf (aref result i) value)
    (finally (return result))))


(defmethod cl-ds.alg.meta:aggregator-constructor ((range csv-range)
                                                  outer-constructor
                                                  (function cl-ds.alg.meta:aggregation-function)
                                                  (arguments list))
  (bind ((outer-fn (call-next-method))
         (header (vellum.header:read-header range)))
    (cl-ds.alg.meta:aggregator-constructor
     (cl-ds.alg:read-original-range range)
     (cl-ds.alg.meta:let-aggregator
         ((inner (cl-ds.alg.meta:call-constructor outer-fn)))

         ((element)
           (vellum.header:with-header (header)
             (block nil
               (let ((row (restart-case (vellum.header:make-row range element)
                            (skip-row ()
                              :report "Skip this row."
                              (return)))))
                 (vellum.header:set-row row)
                 (cl-ds.alg.meta:pass-to-aggregation inner
                                                     row)))))

         ((cl-ds.alg.meta:extract-result inner))

       (cl-ds.alg.meta:cleanup inner))
     function
     arguments)))

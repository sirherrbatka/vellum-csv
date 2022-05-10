(cl:in-package #:vellum-csv)


(defmethod vellum:copy-to ((format (eql :csv))
                           (output stream)
                           input
                           &rest options
                           &key
                             (includes-header-p t)
                             (separator *separator*)
                             (skip-whitespace *skip-whitespace*)
                             (eol *eol*))
  (declare (ignore options))
  (let ((column-count (vellum:column-count input))
        (*separator* separator)
        (*skip-whitespace* skip-whitespace)
        (*eol* eol))
    (when includes-header-p
      (iterate
        (for i from 0 below column-count)
        (collect (vellum:column-name input i)
          into fields)
        (finally (write-csv-line fields output))))
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
                           (write-csv-line fields output))))
                      :in-place t)
    input))


(defmethod vellum:copy-to ((format (eql :csv))
                           output
                           input
                           &rest options
                           &key &allow-other-keys)
  (with-output-to-file (stream output)
    (apply #'vellum:copy-to format stream input options))
  input)


(defmethod vellum:copy-from ((format (eql :csv))
                             path/range
                             &rest options
                             &key
                               (includes-header-p t)
                               (class 'vellum.table:standard-table)
                               (columns '())
                               (body nil)
                               (escape *escape*)
                               (separator *separator*)
                               (quote *quote*)
                               (header (apply #'vellum.header:make-header columns)))
  (declare (ignore options))
  (~> (csv-range path/range
                 :includes-header-p includes-header-p
                 :separator separator
                 :escape escape
                 :quote quote
                 :header header)
      (vellum:to-table :body body
                       :class class)))


(defmethod to-string :around (type value)
  (if (eq :null value)
      ""
      (call-next-method)))


(defmethod from-string :around (type string start end)
  (if (or (= start end) (string= "NULL" string :start2 start :end2 end))
      :null
      (call-next-method)))


(defmethod from-string ((type (eql 'number)) string start end)
  (parse-number string :float-format 'double-float
                       :start start
                       :end end))


(defmethod from-string ((type (eql 'fixnum)) string start end)
  (parse-integer string
                 :start start
                 :end end))


(defmethod from-string ((type (eql 'integer)) string start end)
  (parse-integer string
                 :start start
                 :end end))


(defmethod from-string ((type (eql 'string)) string start end)
  (subseq string start end))


(defmethod from-string ((type (eql 'float)) string start end)
  (parse-float:parse-float string :type 'double-float
                                  :start start
                                  :end end))



(defmethod from-string ((type (eql 'single-float)) string start end)
  (parse-float:parse-float string :type 'single-float
                                  :start start
                                  :end end))


(defmethod from-string ((type (eql 'double-float)) string start end)
  (parse-float:parse-float string :type 'double-float
                                  :start start
                                  :end end))


(defmethod from-string ((type (eql t)) string start end)
  (subseq string start end))


(defmethod from-string (type string start end)
  (if (or (subtypep type 'unsigned-byte) (subtypep type 'signed-byte))
      (parse-integer string
                     :start start
                     :end end)
      (call-next-method)))


(defmethod to-string (type value)
  (princ-to-string value))


(defmethod to-string ((type (eql 'boolean)) value)
  (if value "True" "False"))


(defmethod from-string ((type (eql 'boolean)) string start end)
  (flet ((compare (a b)
           (string= a b :start1 start :end1 end)))
    (switch (string :test compare)
      ("1" t)
      ("0" nil)
      ("T" t)
      ("NIL" nil)
      ("True" t)
      ("False" nil)
      ("F" nil)
      ("Yes" t)
      ("No" nil))))


(defmacro with-stream-input ((stream range) &body body)
  (with-gensyms (!stream)
    `(let* ((,stream (cl-ds.fs:open-stream-designator (path ,range)))
            (,!stream ,stream))
       (unwind-protect
            ,@body
         (close ,!stream)))))


(defmethod cl-ds:traverse ((object csv-range) function)
  (with-stream-input (stream object)
    (let* ((*separator* (separator object))
           (*quote* (csv-quote object))
           (*escape* (escape object))
           (includes-header-p (includes-header-p object))
           (header (vellum.header:read-header object))
           (first-iteration t)
           (result (make-array (vellum.header:column-count header)
                               :initial-element :null))
           (i 0))
      (declare (type simple-vector result))
      (validate-csv-parameters)
      (read-csv stream
                (lambda ()
                  (unless (and includes-header-p first-iteration)
                    (funcall function result))
                  (setf i 0
                        result (make-array (vellum.header:column-count header))
                        first-iteration nil))
                (lambda (elt start end)
                  (unless (and includes-header-p first-iteration)
                    (bind ((data-type (vellum.header:column-type header i))
                           (value  (from-string data-type elt start end)))
                      (unless (or (eq :null value)
                                  (typep value data-type))
                        (error 'vellum.column:column-type-error
                               :expected-type data-type
                               :column i
                               :datum value))
                      (setf (aref result i) value)
                      (incf i))))
                *separator*
                *quote*
                *escape*)))
  object)


(defmethod cl-ds:across ((object csv-range) function)
  (cl-ds:traverse object function))

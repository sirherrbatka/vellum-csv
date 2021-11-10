(in-package :vellum-csv)

(eval-when (:compile-toplevel :load-toplevel :execute)
  (define-constant +cr+ #.(format nil "~A" #\Return)
    :test 'equal)
  (define-constant +lf+ #.(format nil "~A" #\Linefeed)
    :test 'equal)
  (define-constant +crlf+ #.(format nil "~A~A" #\Return #\Linefeed)
    :test 'equal)
  (defparameter *csv-variables* '())) ; list of (var rfc4180-value creativyst-value)

(eval-when (:compile-toplevel :load-toplevel :execute)
  (macrolet
      ((define (var rfc4180 creativyst doc)
         `(progn
            (eval-when (:compile-toplevel :load-toplevel :execute)
              (pushnew `(,',var ,,rfc4180 ,,creativyst) *csv-variables* :key #'car))
            (defparameter ,var ,creativyst ,doc))))
    (define *separator*
  #\, #\,
      "Separator between CSV fields")
    (define *quote*
  #\" #\"
      "delimiter of string data; pascal-like quoted as double itself in a string.")
    (define *unquoted-quotequote*
  nil nil
      "does a pair of quotes represent a quote outside of quotes?
M$, RFC says NIL, csv.3tcl says T")
    (define *loose-quote*
  nil nil
      "can quotes appear anywhere in a field?")
    (define *allow-binary*
  t t
      "do we accept non-ascii data?")
    (define *keep-meta-info*
  nil nil
      "when parsing, include meta information?")
    (define *skip-whitespace*
  nil t
      "shall we skip unquoted whitespace around separators?")
    (define *eol*
      +lf+ +crlf+
      "line ending when exporting CSV")
    ))

(defun char-ascii-text-p (c)
  (<= #x20 (char-code c) #x7E))

(defun valid-eol-p (x)
  (member x (list +cr+ +lf+ +crlf+) :test #'equal))

(defun validate-csv-parameters ()
  (assert (typep *separator* 'character) ())
  (assert (typep *quote* 'character) ())
  (assert (not (eql *separator* *quote*)) ())
  (assert (typep *unquoted-quotequote* 'boolean) ())
  (assert (typep *loose-quote* 'boolean) ())
  (assert (valid-eol-p *eol*) ())
  (assert (typep *keep-meta-info* 'boolean) ())
  (assert (typep *skip-whitespace* 'boolean) ()))

;; For internal use only
(defvar *accept-cr* t "internal: do we accept cr?")
(defvar *accept-lf* t "internal: do we accept lf?")
(defvar *accept-crlf* t "internal: do we accept crlf?")

(define-constant +buffer-size+ 4096)

(defstruct buffered-stream
  stream
  (buffer (make-array +buffer-size+ :element-type 'character) :type simple-string)
  (stream-position +buffer-size+ :type fixnum)
  (buffer-end 0 :type fixnum)
  (all-read nil :type boolean)
  (separator *separator* :type character)
  (quote *quote* :type character)
  (accept-lf *accept-lf* :type boolean)
  (accept-crlf *accept-crlf* :type boolean)
  (accept-cr *accept-cr* :type boolean)
  (skip-whitespace *skip-whitespace* :type boolean)
  (loose-quote *loose-quote* :type boolean)
  (unquoted-quotequote *unquoted-quotequote* :type boolean)
  (keep-meta-info *keep-meta-info* :type boolean))

(declaim (inline buffered-stream-buffer))
(declaim (inline buffered-stream-stream-position))
(declaim (inline buffered-stream-buffer-end))
(declaim (inline buffered-stream-stream))
(declaim (inline buffered-stream-separator))
(declaim (inline buffered-stream-quote))
(declaim (inline buffered-stream-skip-whitespace))
(declaim (inline buffered-stream-accept-cr))
(declaim (inline buffered-stream-accept-crlf))
(declaim (inline buffered-stream-accept-lf))
(declaim (inline buffered-stream-loose-quote))
(declaim (inline buffered-stream-unquoted-quotequote))

(defun buffered-stream-fill-buffer (stream)
  (setf (buffered-stream-buffer-end stream) (read-sequence (buffered-stream-buffer stream)
                                                           (buffered-stream-stream stream))
        (buffered-stream-stream-position stream) 0
        (buffered-stream-all-read stream) (not (= (buffered-stream-buffer-end stream) +buffer-size+))))

(declaim (inline buffered-stream-ensure-buffer))
(defun buffered-stream-ensure-buffer (stream)
  (declare (type buffered-stream stream)
           (optimize (speed 3) (safety 0) (debug 0) (space 0) (compilation-speed 0)))
  (if (< (buffered-stream-stream-position stream)
         (buffered-stream-buffer-end stream))
      t
      (if (buffered-stream-all-read stream)
          nil
          (progn
            (buffered-stream-fill-buffer stream)
            t))))

(defmacro buffered-stream-read (stream &optional (ensured nil))
  (once-only (stream)
    `(block nil
       ,(unless ensured
          `(unless (buffered-stream-ensure-buffer ,stream)
             (return buffered-stream-read nil)))
       (let ((c (aref (buffered-stream-buffer ,stream) (buffered-stream-stream-position ,stream))))
         (incf (buffered-stream-stream-position ,stream))
         (return c)))))

(defmacro buffered-stream-peek (stream &optional (ensured nil))
  (once-only (stream)
    `(block nil
       ,(unless ensured
          `(unless (buffered-stream-ensure-buffer ,stream)
             (return nil)))
       (return (aref (buffered-stream-buffer ,stream) (buffered-stream-stream-position ,stream))))))

; -----------------------------------------------------------------------------

(declaim (inline char-space-p))
(defun char-space-p (separator c)
  "Is character C some kind of white space?
NB: this only handles a tiny subset of whitespace characters,
even if restricted to ASCII. However, it's rather portable,
and is what the creativyst document specifies.
Be careful to not skip a separator, as it could be e.g. a tab!"
  (declare (type (or null character) c)
           (type character separator)
           (optimize (speed 3) (safety 0) (debug 0) (space 0) (compilation-speed 0)))
  (and c
       (or (eql c #\Space)
           (eql c #\Tab))
       (not (eql c separator))))


(defmacro accept (x stream &optional (ensured nil))
  (once-only (stream)
    `(let ((c (buffered-stream-peek ,stream ,ensured)))
       (and (etypecase ,x
              (character (eql ,x c))
              ((or function symbol) (funcall ,x c))
              (integer (eql ,x (char-code c))))
           (progn
             (incf (buffered-stream-stream-position ,stream))
             c)))))


(declaim (inline accept-space))
(defun accept-space (stream)
  (declare (type buffered-stream stream)
           (optimize (speed 3) (safety 0) (debug 0) (space 0) (compilation-speed 0)))
  (let ((separator (buffered-stream-separator stream)))
    (accept (lambda (x) (char-space-p separator x)) stream)))


(declaim (inline accept-spaces))
(defun accept-spaces (stream)
  (loop :for x = (accept-space stream) :while x :collect x))


(defmacro accept-eof (stream &optional (ensured nil))
  `(not (buffered-stream-peek ,stream ,ensured)))


(defun read-csv-line (stream)
  (declare (type buffered-stream stream)
           (optimize (speed 3) (safety 0) (debug 0) (space 0) (compilation-speed 0)))
  "Read one line from STREAM in CSV format, using the current syntax parameters.
  Return a list of strings, one for each field in the line.
  Entries are read as strings;
  it is up to you to interpret the strings as whatever you want."
  (bind ((ss (make-string-output-stream))
         (fields (make-array 32 :element-type t
                                :adjustable t
                                :fill-pointer 0))
         (had-quotes nil)
         (unquoted-quotequote (buffered-stream-unquoted-quotequote stream))
         (quote (buffered-stream-quote stream))
         (separator (buffered-stream-separator stream))
         (loose-quote (buffered-stream-loose-quote stream))
         (skip-whitespace (buffered-stream-skip-whitespace stream))
         (keep-meta-info (buffered-stream-keep-meta-info stream))
         (cr (buffered-stream-accept-cr stream))
         (lf (buffered-stream-accept-lf stream))
         (crlf (buffered-stream-accept-crlf stream))
         ((:flet accept-eol (stream))
          (block nil
            (when (and cr (accept #\Linefeed stream)) (return t))
            (when (or crlf lf)
              (when (accept #\Return stream)
                (when crlf
                  (if (accept #\Linefeed stream t)
                      (return t)
                      (unless cr
                        (error "Carriage-return without Linefeed!"))))
                (return t)))
            nil)))
    (declare (type string-stream ss)
             (type character separator quote)
             (inline accept-eol))
    (labels
        ((do-fields ()
           (setf had-quotes nil)
           (when skip-whitespace
             (accept-spaces stream))
           (cond
             ((and (= 0 (length fields))
                   (or (accept-eol stream)
                       (accept-eof stream t)))
              (done))
             (t
              (do-field-start))))
         (do-field-start ()
           (cond
             ((accept separator stream)
              (add "") (do-fields))
             ((accept quote stream t)
              (cond
                ((and unquoted-quotequote (accept quote stream))
                 (add-char quote)
                 (do-field-unquoted))
                (t
                 (do-field-quoted))))
             (t
              (do-field-unquoted))))
         (do-field-quoted ()
           (setf had-quotes t)
           (cond
             ((accept-eof stream)
              (error "unexpected end of stream in quotes"))
             ((accept quote stream t)
              (cond
                ((accept quote stream)
                 (quoted-field-char quote))
                (loose-quote
                 (do-field-unquoted))
                (t
                 (add (current-string))
                 (end-of-field))))
             (t
              (quoted-field-char (buffered-stream-read stream t)))))
         (quoted-field-char (c)
           (add-char c)
           (do-field-quoted))
         (do-field-unquoted ()
           (if skip-whitespace
               (let ((spaces (accept-spaces stream)))
                 (cond
                   ((or (accept-eol stream)
                        (accept-eof stream t))
                    (add (current-string))
                    (done))
                   ((accept separator stream t)
                    (add (current-string))
                    (do-fields))
                   (t
                    (map () #'add-char spaces)
                    (do-field-unquoted-no-skip))))
               (do-field-unquoted-no-skip)))
         (do-field-unquoted-no-skip ()
           (cond
             ((or (accept-eol stream)
                  (accept-eof stream t))
              (add (current-string))
              (done))
             ((accept separator stream t)
              (add (current-string))
              (do-fields))
             ((accept quote stream t)
              (cond
                ((and unquoted-quotequote
                      (accept quote stream))
                 (add-char quote)
                 (do-field-unquoted))
                (loose-quote
                 (do-field-quoted))
                (t
                 (error "unexpected quote in middle of field"))))
             (t
              (add-char (buffered-stream-read stream t))
              (do-field-unquoted))))
         (end-of-field ()
           ;;#+DEBUG (format t "~&end-of-field~%")
           (when skip-whitespace
             (accept-spaces stream))
           (cond
             ((or (accept-eol stream)
                  (accept-eof stream t))
              (done))
             ((accept separator stream t)
              (do-fields))
             (t
              (error "end of field expected"))))
         (add (x)
           (vector-push-extend
            (if keep-meta-info
                (list x :quoted had-quotes)
                x)
            fields))
         (add-char (c)
           (write-char c ss))
         (current-string ()
           (get-output-stream-string ss))
         (done ()
           fields))
      (declare (inline add-char add))
      (do-fields))))

(defun char-needs-quoting (x)
  (or (eql x *quote*)
      (eql x *separator*)
      (not (char-ascii-text-p x))))

(defun string-needs-quoting (x)
  (and (not (zerop (length x)))
       (or (char-space-p *separator* (char x 0))
     (char-space-p *separator* (char x (1- (length x))))
     (some #'char-needs-quoting x))
       t))

(defun write-csv-lines (lines stream)
  "Given a list of LINES, each of them a list of fields, and a STREAM,
  format those lines as CSV according to the current syntax parameters."
  (dolist (x lines)
    (write-csv-line x stream)))

(defun write-csv-line (fields stream)
  "Format one line of FIELDS to STREAM in CSV format,
  using the current syntax parameters."
  (loop :for x :on fields :do
    (write-csv-field (first x) stream)
    (when (cdr x)
      (write-char *separator* stream)))
  (write-string *eol* stream))

(defun write-csv-field (field stream)
  (etypecase field
    (null t)
    (number (princ field stream))
    (string (write-csv-string-safely field stream))
    (symbol (write-csv-string-safely (symbol-name field) stream))))

(defun write-csv-string-safely (string stream)
  (if (string-needs-quoting string)
      (write-quoted-string string stream)
      (write-string string stream)))

(defun write-quoted-string (string stream)
  (write-char *quote* stream)
  (loop :for c :across string :do
    (when (char= c *quote*)
      (write-char c stream))
    (write-char c stream))
  (write-char *quote* stream))

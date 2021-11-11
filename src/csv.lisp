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
    (define *line-endings*
      (list +crlf+ +lf+) (list +cr+ +lf+ +crlf+)
      "acceptable line endings when importing CSV")
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
  (assert (and *line-endings* (every #'valid-eol-p *line-endings*)) ())
  (assert (typep *keep-meta-info* 'boolean) ())
  (assert (typep *skip-whitespace* 'boolean) ()))

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
  (once-only (x stream)
    `(let ((c (buffered-stream-peek ,stream ,ensured)))
       (if (etypecase ,x
              (character (eql ,x c))
              ((or function symbol) (funcall ,x c))
              (integer (eql ,x (char-code c))))
           (progn
             (incf (buffered-stream-stream-position ,stream))
             c)
           nil))))


(declaim (inline accept-space))
(defun accept-space (stream)
  (declare (type buffered-stream stream)
           (optimize (speed 3) (safety 0) (debug 0) (space 0) (compilation-speed 0)))
  (let ((separator (buffered-stream-separator stream)))
    (accept (lambda (x) (char-space-p separator x)) stream)))


(declaim (inline accept-spaces))
(defun accept-spaces (stream)
  (iterate
    (for x = (accept-space stream))
    (while x)
    (collect x)))


(defmacro accept-eof (stream &optional (ensured nil))
  `(not (buffered-stream-peek ,stream ,ensured)))


(defun read-csv-line (stream free-strings)
  "Read one line from STREAM in CSV format, using the current syntax parameters.
  Return a list of strings, one for each field in the line.
  Entries are read as strings;
  it is up to you to interpret the strings as whatever you want."
  (declare (type buffered-stream stream)
           (optimize (speed 3) (safety 0) (debug 0) (space 0) (compilation-speed 0)))
  (bind ((string-pointer -1)
         (fill-pointer 0)
         ((:flet free-string ())
          (incf (the fixnum string-pointer))
          (unless (< string-pointer (fill-pointer free-strings))
            (vector-push-extend (make-string 512) free-strings))
          (setf fill-pointer 0)
          (aref free-strings string-pointer))
         (ss (free-string))
         (fields (make-array 16 :element-type t
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
         (argument nil))
    (declare (type character separator quote))
    (flet ((add (x)
             (vector-push-extend
              (if keep-meta-info
                  (list x :quoted had-quotes)
                  x)
              fields))
           (add-char (c)
             (let* ((string ss)
                    (size (length string)))
               (declare (type simple-string string))
               (when (= (the fixnum fill-pointer) (the fixnum size))
                 (setf string (make-string (* size 2)))
                 (map-into string #'identity (aref free-strings string-pointer))
                 (setf (aref free-strings string-pointer) string
                       ss string))
               (setf (aref (the simple-string string) fill-pointer) c)
               (incf (the fixnum fill-pointer))))
           (current-string ()
             (lret ((result (make-array fill-pointer
                                        :element-type 'character
                                        :displaced-to ss)))
               (setf ss (free-string))))
           (accept-eol (stream)
             (block nil
               (when (and cr (accept #\Linefeed stream)) (return t))
               (when (or crlf lf)
                 (when (accept #\Return stream)
                   (when crlf
                     (if (accept #\Linefeed stream)
                         (return t)
                         (unless cr
                           (error "Carriage-return without Linefeed!"))))
                   (return t)))
               nil)))
      (declare (inline accept-eol add-char))
      (tagbody
       do-fields
         (progn
           (setf had-quotes nil)
           (when skip-whitespace
             (accept-spaces stream))
           (cond
             ((and (= 0 (length fields))
                   (or (accept-eol stream)
                       (accept-eof stream t)))
              (go done))
             (t
              (go do-field-start))))
       do-field-start
         (cond
           ((accept separator stream)
            (add "") (go do-fields))
           ((accept quote stream t)
            (cond
              ((and unquoted-quotequote (accept quote stream))
               (add-char quote)
               (go do-field-unquoted))
              (t
               (go do-field-quoted))))
           (t
            (go do-field-unquoted)))
       do-field-quoted
         (progn
           (setf had-quotes t)
           (cond
             ((accept-eof stream)
              (error "unexpected end of stream in quotes"))
             ((accept quote stream t)
              (cond
                ((accept quote stream)
                 (setf argument quote)
                 (go quoted-field-char))
                (loose-quote
                 (go do-field-unquoted))
                (t
                 (add (current-string))
                 (go end-of-field))))
             (t
              (setf argument (buffered-stream-read stream t))
              (go quoted-field-char))))
       quoted-field-char
         (progn
           (add-char argument)
           (go do-field-quoted))
       do-field-unquoted
         (if skip-whitespace
             (let ((spaces (accept-spaces stream)))
               (cond
                 ((or (accept-eol stream)
                      (accept-eof stream t))
                  (add (current-string))
                  (go done))
                 ((accept separator stream t)
                  (add (current-string))
                  (go do-fields))
                 (t
                  (map () #'add-char spaces)
                  (go do-field-unquoted-no-skip))))
             (go do-field-unquoted-no-skip))
       do-field-unquoted-no-skip
         (cond
           ((or (accept-eol stream)
                (accept-eof stream t))
            (add (current-string))
            (go done))
           ((accept separator stream t)
            (add (current-string))
            (go do-fields))
           ((accept quote stream t)
            (cond
              ((and unquoted-quotequote
                    (accept quote stream))
               (add-char quote)
               (go do-field-unquoted))
              (loose-quote
               (go do-field-quoted))
              (t
               (error "unexpected quote in middle of field"))))
           (t
            (add-char (buffered-stream-read stream t))
            (go do-field-unquoted)))
       end-of-field
         (progn
           (when skip-whitespace
             (accept-spaces stream))
           (cond
             ((or (accept-eol stream)
                  (accept-eof stream t))
              (go done))
             ((accept separator stream t)
              (go do-fields))
             (t
              (error "end of field expected"))))
       done
         (return-from read-csv-line fields)))))

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

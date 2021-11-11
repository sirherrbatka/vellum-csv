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
    (define *escape* #\" #\"
      "Character used for escaping strings.")
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

;; ---------------------------------------------------------------------------
;;     Title: A very simple CSV Reader
;;   Created: 2021-11-11
;;    Author: Gilbert Baumann
;;   License: MIT style (see below)
;; ---------------------------------------------------------------------------
;;;  (c) copyright 2021 by Gilbert Baumann

;;  Permission is hereby granted, free of charge, to any person obtaining
;;  a copy of this software and associated documentation files (the
;;  "Software"), to deal in the Software without restriction, including
;;  without limitation the rights to use, copy, modify, merge, publish,
;;  distribute, sublicense, and/or sell copies of the Software, and to
;;  permit persons to whom the Software is furnished to do so, subject to
;;  the following conditions:
;;
;;  The above copyright notice and this permission notice shall be
;;  included in all copies or substantial portions of the Software.
;;
;;  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
;;  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
;;  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
;;  IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
;;  CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
;;  TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
;;  SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
(defun csv-to-list (string separator quote escape)
  (let ((result (list))
        (row (list)))
    (with-input-from-string (stream string)
      (read-csv stream
                (lambda ()
                  (push (nreverse row) result)
                  (setf row (list)))
                (lambda (value start end)
                  (push (subseq value start end) row))
                separator
                quote
                escape))
    (nreverse result)))


(declaim (notinline read-csv))
(defun read-csv (stream row-callback column-callback
                 separator quote escape)
  (declare (type character separator quote escape))
  (let* ((minimum-room +buffer-size+)
         (row-callback (ensure-function row-callback))
         (column-callback (ensure-function column-callback))
         (buffer (make-array (* 2 minimum-room) :element-type 'character))
         (columns-counter 0)
         (start 0)                      ;start of our current field
         (fptr 0)                       ;fill pointer of buffer
         (p -1)                         ;reading pointer
         (c #\space))                   ;lookahead
    (declare (type (simple-array character (*)) buffer)
             (type (or null character) c)
             (type fixnum start fptr p minimum-room columns-counter)
             (optimize (speed 3) (safety 0) (compilation-speed 0)
                       (space 0) (debug 0)))
    (labels ((underflow ()
               (replace buffer buffer :start2 start :end2 p)
               (decf p start)
               (decf start start)
               (when (<= (- (length buffer) start) minimum-room)
                 (setq buffer (adjust-array buffer (+ (length buffer) minimum-room)))
                 (setq minimum-room (ash minimum-room 2)))
               (setf fptr (read-sequence buffer stream :start p)))
             (report-result (start end &optional (value buffer))
               (funcall column-callback
                        value
                        start
                        end)
               (incf columns-counter))
             (report-row ()
               (unless (zerop columns-counter)
                 (funcall row-callback))
               (setf columns-counter 0))
             (consume ()
               (when c
                 (incf p)
                 (when (= p fptr) (underflow)))
               (setq c (if (= p fptr) nil (char buffer p))))
             (consume-whitespace ()
               (iterate
                 (while (char-space-p separator c))
                 (consume)))
             (read-field ()
               (consume-whitespace)
               (cond ((eql quote c)
                      (consume)
                      (read-quote-field))
                     (t
                      (setq start p)
                      (iterate
                        (with end = start)
                        (until (or (null c) (eql c separator) (eql c #\newline)))
                        (consume)
                        (unless (char-space-p separator c)
                          (setf end p))
                        (finally (report-result start end))))))
             (read-quote-field ()
               (let ((value (with-output-to-string (stream)
                              (iterate
                                (for escaped = nil)
                                (until (null c))
                                (when (and (not escaped)
                                           (eql c escape))
                                  (consume)
                                  (setf escaped t))
                                (if (eql quote escape)
                                    (if escaped
                                        (cond ((eql c quote)
                                               (princ c stream)
                                               (consume))
                                              (t (finish)))
                                        (cond ((eql c quote)
                                               (consume)
                                               (finish))
                                              (t (princ c stream)
                                                 (consume))))
                                    (cond (escaped
                                           (princ c stream)
                                           (consume))
                                          ((eql c quote)
                                           (consume)
                                           (finish))
                                          (t (princ c stream)
                                             (consume))))))))
                 (report-result 0 (length value) value)
                 (consume-whitespace)
                 (assert (or (null c)
                             (eql #\newline c)
                             (eql separator c)))))
             (read-row ()
               (iterate
                 (read-field)
                 (while (eql c separator))
                 (consume))
               (ecase c ((#\newline nil)))
               (consume)))
      (declare (inline underflow consume read-field read-row
                       consume-whitespace report-result))
      (consume)
      (iterate
        (until (null c))
        (read-row)
        (report-row)))))


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

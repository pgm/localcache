__author__ = 'pmontgom'

import flask
import tempfile
import re
import sqlite3
import threading
import os
import sys

from flask import Flask, render_template, request, g
from boto.s3.connection import S3Connection

app = Flask(__name__)

DB_INIT_STATEMENTS = ["CREATE TABLE CACHED (path STRING primary key, local_path STRING)"]

class TransactionContext:
    def __init__(self, connection, lock):
        self.connection = connection
        self.depth = 0
        self.lock = lock

    def __enter__(self):
        if self.depth == 0:
            self.lock.acquire()
        self._db = self.connection.cursor()
        self.depth += 1
        return self._db

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.depth -= 1
        if self.depth == 0:
            self.connection.commit()
            self._db.close()
            self.lock.release()

class Store:
    def __init__(self, db_path):
        new_db = not os.path.exists(db_path)

        self._connection = sqlite3.connect(db_path, check_same_thread=False)
        self._db = self._connection.cursor()
        self._lock = threading.Lock()
        self._active_transaction = None
        #self._cv_created = threading.Condition(self._lock)

        if new_db:
            for statement in DB_INIT_STATEMENTS:
                self._db.execute(statement)

    def transaction(self):
        if self._active_transaction is None:
            self._active_transaction = TransactionContext(self._connection, self._lock)
        return self._active_transaction

    def get_local_path(self, path):
        with self.transaction() as db:
            db.execute("SELECT local_path FROM CACHED where path = ?", [path])
            row = db.fetchone()
            if row == None:
                return None
            return row[0]

    def set_local_path(self, path, local_path):
        with self.transaction() as db:
            db.execute("INSERT INTO CACHED (path, local_path) VALUES (?, ?)", [path, local_path])

    def close(self):
        self._connection.close()

class Cache:
    def __init__(self, aws_access_key, aws_secret_key, store, temp_dir):
        self.conn = S3Connection(aws_access_key, aws_secret_key)
        self.store = store
        self.temp_dir = temp_dir

    def fetch(self, path):
        m = re.match("s3://([^/]+)/(.+)", path)
        assert m != None
        bucket = m.group(1)
        key_name = m.group(2)

        local_path = tempfile.NamedTemporaryFile(dir=self.temp_dir, delete=False).name
        bucket = self.conn.get_bucket(bucket)
        key = bucket.get_key(key_name, validate=False)
        key.get_contents_to_filename(local_path)
        return local_path

    def resolve(self, path):
        local_path = self.store.get_local_path(path)
        if local_path != None:
            return local_path
        local_path = self.fetch(path)
        self.store.set_local_path(path, local_path)
        return local_path

    def close(self):
        self.store.close()

@app.before_request
def before_request():
    db_path = app.config["STORAGE_PATH"]+"/db.sqlite3"
    store = Store(db_path)
    g.cache = Cache(app.config['AWS_ACCESS_KEY_ID'], app.config['AWS_SECRET_ACCESS_KEY'], store, app.config["STORAGE_PATH"])

@app.teardown_request
def teardown_request(exception):
    cache = getattr(g, 'cache', None)
    if cache is not None:
        cache.close()

@app.route("/get_local")
def fetch():
    path = request.args["path"]
    return g.cache.resolve(path)

app.config["DEBUG"] = False
app.config.from_pyfile(sys.argv[1])

if not os.path.exists(app.config["STORAGE_PATH"]):
    os.makedirs(app.config["STORAGE_PATH"])

app.run(debug=app.config["DEBUG"])

# -*- coding: utf-8 -*-
"""
Created on Fri Mar 17 16:02:42 2023

@author: soso2
"""

from flask import Flask

app = Flask(__name__)

@app.route('/')
def hello():
    return 'Hello, World!'

if __name__ == '__main__':
    app.run()




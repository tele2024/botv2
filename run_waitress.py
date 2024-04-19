# -*- coding: utf-8 -*-
"""
Created on Sat Oct 28 16:41:11 2023

@author: soso2
"""


from waitress import serve
from server import app  # Import your Flask app instance from your_app.py

def serve_flask_app():
    serve(app, host='0.0.0.0', port=8000)
    


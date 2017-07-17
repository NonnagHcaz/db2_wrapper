# -*- coding: utf-8 -*-

import sys
import os
sys.path.insert(0,
                os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from db2_wrapper import db2_wrapper

BASEDIR = './tests'
DATA_DIR = os.path.join(BASEDIR, 'dat')

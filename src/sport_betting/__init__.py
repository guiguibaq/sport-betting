# coding: utf-8
import os
from pathlib import Path

PROJECT_DIR = Path(__file__).parents[2]
DATA_DIR = os.path.join(PROJECT_DIR, "data")
CONFIG_DIR = os.path.join(PROJECT_DIR, "config")

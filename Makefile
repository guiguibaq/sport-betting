#!/bin/bash

venv:
	python -m venv ./venv;
	. ./activate_venv && python -m pip install pip --upgrade;
	. ./activate_venv && python -m pip install -r requirements.txt

update: venv
	. ./activate_venv && python -m pip install -r requirements.txt


#!/bin/bash

args = `arg="$(filter-out $@,$(MAKECMDGOALS))" && echo $${arg:-${1}}`

venv:
	python -m venv ./venv;
	. ./activate_venv && python -m pip install -q pip --upgrade;
	. ./activate_venv && python -m pip install -q -r requirements.txt

update: venv
	. ./activate_venv && python -m pip install -q -r requirements.txt

data: venv
	. ./activate_venv && cd ./src && python -m sport_betting.data_retrieval.run_workflow $(call args,2020) || true

.PHONY: venv update data
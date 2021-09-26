# coding: utf-8
import luigi
import os
import configparser
from sport_betting import CONFIG_DIR

import re


class APIConfig(luigi.Config):
    path = luigi.Parameter(default=os.path.join(CONFIG_DIR, "config.cfg"))

    def get_config(self):
        config = configparser.ConfigParser()
        config.read(self.path)
        return config

    @property
    def football_data_api_key(self):
        return self.get_config()['FootballData']['api_key']

    @property
    def betfair_user(self):
        return self.get_config()['Betfair']['email']

    @property
    def betfair_pwd(self):
        return self.get_config()['Betfair']['password']

    @property
    def betfair_api_key(self):
        return self.get_config()['Betfair']['api_key']

    @property
    def betfair_certs(self):
        return self.get_config()['Betfair']['certs_dir']

    @property
    def betfair_cert_file(self):
        cert = None
        for file in os.listdir(self.betfair_certs):
            ext = os.path.splitext(file)[-1]
            if ext in [".crt", ".cert"]:
                cert = os.path.join(self.betfair_certs, file)

        if cert:
            return cert
        else:
            raise ValueError("Missing certificate (.crt, .cert) in certificate directory {}".format(self.betfair_certs))

    @property
    def betfair_cert_key(self):
        key = None
        for file in os.listdir(self.betfair_certs):
            ext = os.path.splitext(file)[-1]
            if ext == ".key":
                key = os.path.join(self.betfair_certs, file)

        if key:
            return key
        else:
            raise ValueError("Missing key file (.key) in certificate directory {}".format(self.betfair_certs))

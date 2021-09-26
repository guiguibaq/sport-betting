# coding: utf-8
import luigi
import os
import configparser
from sport_betting import CONFIG_DIR


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
        return self.get_config()['Betfair']['cert_file']

    @property
    def betfair_cert_key(self):
        return self.get_config()['Betfair']['key_file']

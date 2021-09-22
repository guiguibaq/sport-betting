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
    def football_data_token(self):
        return self.get_config()['FootballData']['token']

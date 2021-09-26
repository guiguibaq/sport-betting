# coding: utf-8
import os

import requests

from sport_betting.data_retrieval.config.config import APIConfig


def get_session_token(login_url: str = "https://identitysso-cert.betfair.com/api/certlogin") -> str:
    """
    Get session token from Betfair
    :param login_url: SSO login url from Betfair
    :return: Session token
    """
    # Get authentication data from config
    api_cfg = APIConfig()
    cert = (os.path.join(api_cfg.betfair_certs, api_cfg.betfair_cert_file),
            os.path.join(api_cfg.betfair_certs, api_cfg.betfair_cert_key))
    login_data = {
        "username": api_cfg.betfair_user,
        "password": api_cfg.betfair_pwd
    }

    # Get token from login endpoint
    r_login = requests.post(login_url,
                            data=login_data,
                            headers={'X-Application': "SportsBettingYoloer"},
                            cert=cert)

    # Extract token
    return r_login.json()['sessionToken']

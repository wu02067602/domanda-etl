import logging
import os
import subprocess
import psycopg2

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class Config:
    PROJECT_ID = os.getenv("PROJECT_ID", "testing-cola-rd")
    DATASET_ID = os.getenv("DATASET_ID", "domanda")
    TABLE_NAME = os.getenv("TABLE_NAME", "flight_ticket_price_compare")
    
    # IAP tunnel 配置
    IAP_ZONE = os.getenv("IAP_ZONE", "asia-east1-b")
    IAP_PROJECT = os.getenv("IAP_PROJECT", "testing-cola-rd")
    IAP_INSTANCE = os.getenv("IAP_INSTANCE", "testing-proxyvm")
    IAP_PORT = os.getenv("IAP_PORT", "5432")
    IAP_LOCAL_PORT = os.getenv("IAP_LOCAL_PORT", "5432")

    @staticmethod
    def setup_iap_tunnel():
        """設置 IAP tunnel 連接"""
        try:
            if bool(os.getenv("IS_CLOUD", False)):
                cmd = f"gcloud compute start-iap-tunnel --zone {Config.IAP_ZONE} --project {Config.IAP_PROJECT} {Config.IAP_INSTANCE} {Config.IAP_PORT} --local-host-port=localhost:{Config.IAP_LOCAL_PORT}"
                logging.info("Starting IAP tunnel...")

                process = subprocess.Popen(cmd, shell=True)
                return process
            else:
                return None
        except Exception as e:
            logging.error(f"Failed to setup IAP tunnel: {str(e)}")
            raise

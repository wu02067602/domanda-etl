import config
import logging
from etl.pipeline import Pipeline

def main():
    try:
        # 設置 IAP tunnel
        iap_process = config.Config.setup_iap_tunnel()
        
        # 執行 pipeline
        pipeline = Pipeline(project_id=config.Config.PROJECT_ID)
        pipeline.run()
        
    except Exception as e:
        logging.error(f"Pipeline execution failed: {str(e)}")
        raise
    finally:
        # 確保程序結束時關閉 IAP tunnel
        if 'iap_process' in locals() and iap_process is not None:
            iap_process.terminate()

if __name__ == "__main__":
    main()

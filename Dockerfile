# 選擇 python base image
FROM python:3.10-slim

# 安裝必要的系統套件
RUN apt-get update && apt-get install -y \
    curl \
    gnupg \
    ca-certificates \
    lsb-release \
    postgresql-server-dev-all \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# 安裝 Google Cloud SDK
RUN install -d -m 0755 /usr/share/keyrings && \
    curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg && \
    echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee /etc/apt/sources.list.d/google-cloud-sdk.list > /dev/null && \
    apt-get update -y && \
    apt-get install -y google-cloud-sdk && \
    rm -rf /var/lib/apt/lists/*

# 複製 requirements.txt
COPY requirements.txt .

# 安裝 Python 相依套件
RUN pip install -r requirements.txt

# 建立存放憑證的目錄
RUN mkdir -p /tmp/keys

# # 複製金鑰檔案
# COPY key-dev-domanda-etl-data-test.json /tmp/keys/

# 複製程式碼
COPY . .

# 設定環境變數
# ENV GOOGLE_APPLICATION_CREDENTIALS=/tmp/keys/key-dev-domanda-etl-data-test.json

# # 啟用服務帳戶
# RUN gcloud auth activate-service-account --key-file=$GOOGLE_APPLICATION_CREDENTIALS

# 設定執行進入點
# CMD ["/bin/bash", "-c", "set -e; gcloud compute start-iap-tunnel --zone asia-east1-b --project flypa-tw-testing proxyvm 5432 --local-host-port=localhost:5432 & sleep 3; python main.py"]
CMD ["python", "main.py"]

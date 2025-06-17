FROM bitnami/spark:3.4.1

USER root

WORKDIR /app

RUN install_packages python3 python3-pip

COPY . .

RUN pip3 install --no-cache-dir -r requirements.txt

CMD ["bash"]
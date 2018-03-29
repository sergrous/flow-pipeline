FROM debian:stretch

ARG DOWNLOAD_URL1=https://github.com/cloudflare/flow-pipeline/releases/download/v1.0.0/consumer-linux-x86_64
ARG DOWNLOAD_URL2=https://github.com/cloudflare/flow-pipeline/releases/download/v1.0.0/processor-linux-x86_64

RUN apt-get update && \
    apt-get -y install curl && \
    apt-get clean && \
    curl -L ${DOWNLOAD_URL1} > /consumer && \
    curl -L ${DOWNLOAD_URL2} > /processor && \
    chmod +x /consumer && \
    chmod +x /processor

ENTRYPOINT [ "/consumer" ]
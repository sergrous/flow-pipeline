FROM debian:stretch

COPY consumer /
COPY processor /

RUN apt-get update && \
    apt-get clean && \
    chmod +x /consumer && \
    chmod +x /processor

ENTRYPOINT [ "/consumer" ]
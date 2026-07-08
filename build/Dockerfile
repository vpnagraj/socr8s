FROM docker:27-dind

RUN apk add --no-cache \
    bash \
    git \
    curl

WORKDIR /src

COPY build-images.sh /src/build-images.sh
RUN chmod +x /src/build-images.sh

RUN mkdir /workspace

ENTRYPOINT ["/src/build-images.sh"]

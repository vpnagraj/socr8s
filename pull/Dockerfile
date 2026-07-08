FROM docker:27-dind

RUN apk add --no-cache bash

WORKDIR /src

COPY pull-images.sh /src/pull-images.sh
RUN chmod +x /src/pull-images.sh

ENTRYPOINT ["/src/pull-images.sh"]

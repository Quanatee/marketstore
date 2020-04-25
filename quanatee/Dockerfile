#
# STAGE 1
#
# Uses a Go image to build a release binary.
#
FROM golang:1.14.2-buster as builder
ARG tag=latest
ARG INCLUDE_PLUGINS=true
ENV DOCKER_TAG=$tag
ENV GOPATH=/go

WORKDIR /go/src/github.com/alpacahq/marketstore/
ADD ./ ./
RUN make vendor
RUN if [ "$INCLUDE_PLUGINS" = "true" ] ; then make build plugins ; else make build ; fi

#
# STAGE 2
#
# Create final image
#
FROM debian:10.3
WORKDIR /
COPY --from=builder /go/src/github.com/alpacahq/marketstore/marketstore /bin/
# copy plugins if any
COPY --from=builder /go/bin /bin/
ENV GOPATH=/

# RUN ["marketstore", "init"]
# RUN mv mkts.yml /etc/
VOLUME /data
EXPOSE 5993

# ENTRYPOINT ["marketstore"]
# CMD ["start", "--config", "/etc/mkts.yml"]

COPY /quanatee/mkts.yml .
ENTRYPOINT sed -i "s|<polygon_api_key>|${POLYGON_API_KEY}|g" mkts.yml && sed -i "s|<tiingo_api_key>|${TIINGO_API_KEY}|g" mkts.yml && sed -i "s|<twelve_api_key>|${TWELVE_API_KEY}|g" mkts.yml && marketstore start --config mkts.yml && /bin/bash
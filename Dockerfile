FROM --platform=$TARGETPLATFORM golang:1.13.5-stretch as devel
ARG BUILD_ARGS
COPY / /go/src/
RUN cd /go/src/ && make build-local BUILD_ARGS=$BUILD_ARGS

FROM --platform=$TARGETPLATFORM busybox
COPY --from=devel /go/src/baetyl-broker /bin/
ENTRYPOINT ["baetyl-broker"]

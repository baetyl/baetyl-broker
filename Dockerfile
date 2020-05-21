FROM --platform=$TARGETPLATFORM golang:1.14.3-stretch as devel
COPY / /go/src/
RUN cd /go/src/ && make all

FROM --platform=$TARGETPLATFORM busybox
COPY --from=devel /go/src/baetyl-broker /bin/
ENTRYPOINT ["baetyl-broker"]

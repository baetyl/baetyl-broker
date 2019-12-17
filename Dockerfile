FROM --platform=$TARGETPLATFORM golang:1.13.5-stretch as build
ARG RACE
COPY / /build/
RUN cd /build/ && make static GO_RACE=$RACE

FROM --platform=$TARGETPLATFORM busybox
COPY --from=build /build/baetyl-broker /bin/
ENTRYPOINT ["baetyl-broker"]

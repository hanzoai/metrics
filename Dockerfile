# Build vmagent from VictoriaMetrics source
FROM golang:1.26.5-alpine AS builder

ARG TARGETOS=linux
ARG TARGETARCH

RUN apk add --no-cache make git

WORKDIR /src
RUN git clone --depth 1 --branch v1.117.1 https://github.com/VictoriaMetrics/VictoriaMetrics.git .

RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath \
    -ldflags "-s -w -X github.com/VictoriaMetrics/VictoriaMetrics/lib/buildinfo.Version=v1.117.1" \
    -o /vmagent ./app/vmagent

# One directory in an empty image: the static binary and the files it reads;
# nothing else is present to run, so nothing else can be run.
FROM alpine:3.22 AS root
RUN apk add --no-cache ca-certificates tzdata

FROM scratch
COPY --from=root /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=root /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=builder /vmagent /usr/local/bin/vmagent
USER 65532:65532
EXPOSE 8429
ENTRYPOINT ["/usr/local/bin/vmagent"]

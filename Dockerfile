# Build vmagent from VictoriaMetrics source
FROM golang:1.24-alpine AS builder

ARG TARGETOS=linux
ARG TARGETARCH

RUN apk add --no-cache make git

WORKDIR /src
RUN git clone --depth 1 --branch v1.117.1 https://github.com/VictoriaMetrics/VictoriaMetrics.git .

RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath \
    -ldflags "-s -w -X github.com/VictoriaMetrics/VictoriaMetrics/lib/buildinfo.Version=v1.117.1" \
    -o /vmagent ./app/vmagent

FROM alpine:3.21
LABEL maintainer="hanzoai"

RUN apk add --no-cache ca-certificates tzdata && \
    rm -rf /var/cache/apk/*

COPY --from=builder /vmagent /usr/local/bin/vmagent

EXPOSE 8429

ENTRYPOINT ["/usr/local/bin/vmagent"]

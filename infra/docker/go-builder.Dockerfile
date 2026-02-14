FROM golang:1.22-bullseye
WORKDIR /app
ARG TARGETOS=linux
ARG TARGETARCH=amd64
ENV CGO_ENABLED=0
ENV GOOS=${TARGETOS}
ENV GOARCH=${TARGETARCH}
COPY go.* ./
RUN go mod download
COPY go ./go
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    go build -o /out/ws_bridge ./go/ingestion/ws_bridge && \
    go build -o /out/bar_builder_1s ./go/compute/bar_builder_1s && \
    go build -o /out/bar_aggregator_1m ./go/compute/bar_aggregator_1m && \
    go build -o /out/bar_aggregator_multi ./go/compute/bar_aggregator_multi

#
# BUILDER
#
FROM docker.io/library/golang:1.27.1 AS builder

WORKDIR /buildroot

COPY go.mod go.mod
COPY go.sum go.sum
RUN go mod download

# Copy the source.
COPY pkg/ pkg/
COPY cmd/ cmd/

# Disable CGO.
ENV CGO_ENABLED=0

# Build the image.
RUN go build -trimpath -o build/ontap-cosi-driver cmd/ontap-cosi-driver/*.go

#
# FINAL IMAGE
#
FROM gcr.io/distroless/static:latest AS runtime

LABEL org.opencontainers.image.maintainers="Joey Loman"
LABEL org.opencontainers.image.description="Container Object Storage Interface (COSI) ONTAP Driver"
LABEL org.opencontainers.image.title="COSI ONTAP Driver"
LABEL org.opencontainers.image.source="https://github.com/joeyloman/ontap-cosi-driver"
LABEL org.opencontainers.image.licenses="APACHE-2.0"

COPY --from=builder /buildroot/build/ontap-cosi-driver /ontap-cosi-driver

ENTRYPOINT [ "/ontap-cosi-driver" ]

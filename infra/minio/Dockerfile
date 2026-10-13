FROM golang:1.24.2-bookworm AS build
RUN CGO_ENABLED=0 GOBIN=/out go install github.com/minio/minio@RELEASE.2025-04-22T22-12-26Z
FROM debian:bookworm-slim
COPY --from=build /out/minio /usr/local/bin/minio
EXPOSE 9000 9001
ENTRYPOINT ["minio"]

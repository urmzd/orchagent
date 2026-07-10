# Build the static dispatch binary.
FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/dispatch ./cmd/dispatch

# Minimal runtime: one binary, no shell, non-root.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/dispatch /usr/local/bin/dispatch
EXPOSE 8484
ENTRYPOINT ["dispatch"]
CMD ["serve", "--workspace", "/workspace"]

# Build the static legatus binary.
FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/legatus ./cmd/legatus

# Minimal runtime: one binary, no shell, non-root.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/legatus /usr/local/bin/legatus
EXPOSE 8484
ENTRYPOINT ["legatus"]
CMD ["serve", "--workspace", "/workspace"]

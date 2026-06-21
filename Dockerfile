FROM golang:1.25-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o /out/researchd ./cmd/researchd

FROM alpine:3.22

RUN adduser -D -H app
USER app
WORKDIR /app
COPY --from=build /out/researchd /app/researchd
ENTRYPOINT ["/app/researchd"]

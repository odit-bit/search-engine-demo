########################################################
# STEP 1 use a temporary image to build a static binary
########################################################
FROM golang:1.21-alpine as build-stage

RUN apk add --no-cache git
RUN apk --no-cache add ca-certificates

WORKDIR /

COPY app app/
COPY x x/
COPY go.mod .
COPY go.sum .
COPY *.go .
RUN go mod download
RUN go test

# make static image
ENV CGO_ENABLED=0
ENV GOOS=linux
ENV GOARCH=amd64
RUN go build -o /rpcCrawler ./app/rpc/main.go


########################################################
# STEP 2 create distroless image with trusted certs
########################################################
FROM gcr.io/distroless/base-debian11 AS build-release-stage
# RUN apk update && apk add ca-certificates && rm -rf /var/cache/apk/*
WORKDIR /

COPY --from=build-stage /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build-stage /rpcCrawler /rpcCrawler

EXPOSE 8080

ENTRYPOINT [ "./rpcCrawler" ]
# CMD [ "./monolith" ]
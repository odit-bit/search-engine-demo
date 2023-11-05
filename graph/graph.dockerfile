########################################################
# STEP 1 use a temporary image to build a static binary
########################################################
FROM golang:1.21-alpine as build-stage

RUN apk add --no-cache git
RUN apk --no-cache add ca-certificates

WORKDIR /

COPY graph graph
COPY go.mod .
COPY go.sum .

# RUN go mod download

# make static image
ENV CGO_ENABLED=0
ENV GOOS=linux
ENV GOARCH=amd64
RUN go build -o graphServer ./graph/


########################################################
# STEP 2 create distroless image with trusted certs
########################################################
FROM gcr.io/distroless/base-debian11 AS build-release-stage
# RUN apk update && apk add ca-certificates && rm -rf /var/cache/apk/*
WORKDIR /

COPY --from=build-stage /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build-stage graphServer graphServer

EXPOSE 8181

ENTRYPOINT [ "./graphServer" ]
# CMD [ "./monolith" ]
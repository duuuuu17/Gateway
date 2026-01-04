# build golang image with go version 1.25.5
FROM golang:1.25.5-alpine AS builder

# build parameters
ENV GO111MODULE=on
ENV GOPROXY=https://goproxy.cn,direct
ENV CGO_ENABLED=0
ENV GOOS=linux
ENV GOARCH=amd64

# set work directory
WORKDIR /app

# copy go mod and go sum files
COPY go.mod .
COPY go.sum .
# download go mod dependencies
RUN go mod download

# copy source files
COPY . .
# build the binary
RUN go build -ldflags="-s -w" -o llm-router ./main.go

FROM alpine:latest
# annotations
LABEL maintainer="duuuuu17 <ethandu17@qq.com>"
LABEL version="alpha"
LABEL author="duuuuu17"
LABEL email="ethandu17@qq.com"

WORKDIR /app

COPY --from=builder /app/llm-router /app/llm-router

RUN chmod u+x /app/llm-router

EXPOSE 8080

CMD ["./llm-router"]
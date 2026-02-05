FROM golang:1.24-alpine AS build
WORKDIR /app
COPY go.mod go.sum ./
RUN apk --no-cache add git build-base libheif-dev
RUN go mod download
COPY . .
RUN CGO_ENABLED=1 go build -ldflags="-s -w" -o order-services .

FROM alpine:3.19
WORKDIR /app
RUN apk --no-cache add ca-certificates wget libheif tzdata
COPY --from=build /app/order-services /app/order-services
EXPOSE 8086
ENV HTTP_ADDR=:8086
CMD ["/app/order-services"]

FROM golang:1.19-alpine as development

ENV CGO_ENABLED=1

WORKDIR /go/src/app

RUN apk update && apk add git
RUN go get github.com/cespare/reflex
COPY . .
RUN go build -o gfw-tool main.go

CMD ["reflex", "-c", "./reflex.conf"]

FROM alpine AS build
WORKDIR /opt/
COPY --from=development /go/src/app/gfw-tool gfw-tool
ENTRYPOINT ["/opt/gfw-tool"]
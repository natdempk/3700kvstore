all:
		$(RM) 3700kvstore
		GOPATH=${PWD} go get github.com/satori/go.uuid
		GOPATH=${PWD} go build -o 3700kvstore main.go

clean:
		$(RM) 3700kvstore

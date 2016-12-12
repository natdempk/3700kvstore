all:
		$(RM) 3700kvstore
		export GOPATH=${PWD}
		go get github.com/satori/go.uuid
		go build -o 3700kvstore main.go

clean:
		$(RM) 3700kvstore

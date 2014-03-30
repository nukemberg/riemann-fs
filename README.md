# RiemannFS

RiemannFS is a fuse filesystem that exposes the [Riemann](http://riemann.io/) index on a mounted filesystem.

## Usage

Build the binary

    go get github.com/avishai-ish-shalom/riemann-fs && go build github.com/avishai-ish-shalom/riemann-fs

Then run the mount command

    ./riemann-fs -host=192.168.2.31 -port=5555 /mnt/riemann

## License

GPL v3
batchhat
========

A batch sender to send metrics to StatHat using its [bulk JSON API](https://www.stathat.com/docs/api#json)

This is helpful when sending a ton of metrics; in our case, sending up to hundreds of metrics per second per
process. Sending so many metrics individually over HTTP adds a ton of overhead and taking up resources that
could be used by your app.

Testing
-------

```
# You must install ginko and the matching library gomega
$ go get github.com/onsi/ginkgo/ginkgo
$ go get github.com/onsi/gomega

# Run the tests!
$ ginkgo -r --randomizeAllSpecs --skipMeasurements --cover --trace --race

# While writing new tests, it's helpful to have ginkgo watch for changes
$ ginkgo watch -r --randomizeAllSpecs --trace --race
```

License
-------

See the `LICENSE` file. (Spoiler alert: MIT License)

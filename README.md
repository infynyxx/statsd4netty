### Experimental StatsD Client using Netty 4.x

[![Build Status](https://secure.travis-ci.org/infynyxx/statsd4netty.png)](http://travis-ci.org/infynyxx/statsd4netty)

``` java
StatsDClient client = new StatsDClient("127.0.0.1", 8125);
ChannelFuture future = client.increment("stats1");
client.timing("stats1", 23422); // in MS
client.gauge("stats1", 787);
```

TwitStorm
=========

Overview
--------

TwitStorm is a simple Storm topology that calculates the popularity of hashtags appearing in the [Twitter Sample Stream][samplestream]. Specifically, it connects to a local redis instance and creates a sorted set called `hashtags`. Every two minutes, it adds each hashtag it's seen within the last ten minutes to `hashtags` with a rank equal to the number of times it was seen in that window.

 [samplestream]: https://dev.twitter.com/docs/api/1.1/get/statuses/sample

Assumptions
-----------

There were several assumptions made in this process, most notably:

 - _That the popularity of hashtags wouldn't bottom out immediately_: Records are never removed from redis, but as long as a hashtag becomes less popular before disappearing entirely, this should not significantly impact the top-ten readout.
 - _That the tweets are received shortly after they are created_: Due to time zone issues, it was simpler to use the tweets' receipt timestamps rather than their creation timestamps.

Setup
-----

This project was written in [IntelliJ][idea], and no attempt was made to make it run independently. I recommend downloading the free community edition of IntelliJ and running it from there. All java dependencies are specified in the Maven pom file. Before running, make sure you set up your run profile in IntelliJ to contain the JVM flags `-Dtwitter.username=YOUR_TWITTER_USERNAME` and `-Dtwitter.password=YOUR_TWITTER_PASSWORD`.

 [idea]: http://www.jetbrains.com/idea/

TwitStorm also requires a running redis server on localhost.

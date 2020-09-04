# kafkacopy

kafkacopy - Apache [Kafka](https://kafka.apache.org/) command line tool.

kafkacopy is a tool which plays well with other *nix command line programs such as`grep`, `awk` and [`jq`](https://stedolan.github.io/jq/).

Current version: pre-alpha / work-in-progress.  

## Requirements
* Java 11, or later

## Building and development

Build uses [`mill`](http://www.lihaoyi.com/mill/):
```
# run tests
mill kafkacopy.test
# create a runnable standalone jar
mill kafkacopy.assembly
# to run
java -jar out/kafkacopy/assembly/dest/out.jar --help
```
To configure IntelliJ IDEA
```
mill mill.scalalib.GenIdea/idea
```
Start local Kafka broker (for manual testing)
```
mill kafkacopy.test.runMain kafka.LocalKafka
```
To check [REUSE](https://reuse.software/) compliance
```
reuse lint
```

## Data format
New line delimited json is used for file and stdin/stdout. Binary fields are be rendered as plaintext, json or base64.

See [kafkacopy/test/resources/sample1.jsonl](./kafkacopy/test/resources/sample1.jsonl)

# Tutorial

Yoy can start local kafka broker like this, if needed:
```
$ mill kafkacopy.test.runMain kafka.LocalKafka
...
Local kafka running at: 127.0.0.1:35489
ctrl-c to terminate
```
Create a topic and list topics (adjust bootstrap server).
```
$ java -jar out/kafkacopy/assembly/dest/out.jar mktopic -p bootstrap.servers=127.0.0.1:35489 demo/foo
$ java -jar out/kafkacopy/assembly/dest/out.jar ls -p bootstrap.servers=127.0.0.1:35489 demo
foo
```
The last argument is kafka broker (configuration file) name.
We haven't created configuration file yet but we still need to pick some name.

Configuration file is a Java [properties](https://docs.oracle.com/javase/tutorial/essential/environment/properties.html) file for kafka. For details, see:
* https://kafka.apache.org/documentation/#producerconfigs
* https://kafka.apache.org/documentation/#consumerconfigs
* https://kafka.apache.org/documentation/#adminclientconfigs

For demo purposes we set `KAFKACOPY_HOME` environment variable to current directory and create a `demo.properties`:
```
$ echo bootstrap.servers=127.0.0.1:35489 > demo.properties
$ KAFKACOPY_HOME=. java -jar out/kafkacopy/assembly/dest/out.jar ls demo
foo
```
Write some events to topic we created earlier:
```
$ cat kafkacopy/test/resources/sample1.jsonl
{"topic":"a","partition":3,"offset":1,"headers":[{"key":"b","value":"h"}],"key":"k","value":"v","timestamp": 5}
{"topic":"a","headers":[{"key":"b","value":null}],"key":"i","value":"j","timestamp": 7}
{"topic":"b","key":null,"value":"x"}
$ KAFKACOPY_HOME=. java -jar out/kafkacopy/assembly/dest/out.jar copy @kafkacopy/test/resources/sample1.jsonl demo/foo
```
To print topic contents:
```
$ KAFKACOPY_HOME=. java -jar out/kafkacopy/assembly/dest/out.jar copy demo/foo @-
{"topic":"foo","headers":[{"key":"b","value":"h"}],"key":"k","value":"v","timestamp":5}
{"topic":"foo","headers":[{"key":"b","value":null}],"key":"i","value":"j","timestamp":7}
{"topic":"foo","key":null,"value":"x","timestamp":1598205751494}
```
To create a new topic, copy messages from topic foo to bar and finally copy topic bar to stdout:
```
$ KAFKACOPY_HOME=. java -jar out/kafkacopy/assembly/dest/out.jar mktopic demo/bar
$ KAFKACOPY_HOME=. java -jar out/kafkacopy/assembly/dest/out.jar copy demo/foo demo/bar
$ KAFKACOPY_HOME=. java -jar out/kafkacopy/assembly/dest/out.jar copy demo/bar @-
{"topic":"bar","headers":[{"key":"b","value":"h"}],"key":"k","value":"v","timestamp":5}
{"topic":"bar","headers":[{"key":"b","value":null}],"key":"i","value":"j","timestamp":7}
{"topic":"bar","key":null,"value":"x","timestamp":1598205751494}
```

## License
[Apache License, Version 2.0](./LICENSES/Apache-2.0.txt)
<!--
SPDX-FileCopyrightText: 2020 Juha Heljoranta <juha.heljoranta@iki.fi

SPDX-License-Identifier: Apache-2.0
-->

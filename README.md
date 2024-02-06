## IP Counter

#### Build
```./gradlew build```
#### Run
```java -jar build/libs/app.jar <path-to-log-file>```

path-to-log-file - absolute path to the file containing ip addresses

with current config and on mac m1 16Gb of RAM it takes about 6 minutes to process whole file with 120GB of data (app takes about 2.5GB of RAM)

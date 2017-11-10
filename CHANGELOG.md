## 1.1.2
- Fix a Bug in the S3 Key generation
- Enable shipping throug SNS Topic (needs another toJSON)
## 1.1.1
- Added the ability to remove objects from S3 after processing.
- Workaround an issue with the Ruby autoload that causes "uninitialized constant `Aws::Client::Errors`" errors.

## 1.1.0
- Logstash 5 compatibility

## 1.0.3
- added some metadata to the event (bucket and object name as commited by joshuaspence)
- also try to unzip files ending with ".gz" (ALB logs are zipped but not marked with proper Content-Encoding)

## 1.0.2
- fix for broken UTF-8 (so we won't lose a whole s3 log file because of a single invalid line, ruby's split will die on those)

## 1.0.1
- same (because of screwed up rubygems.org release)

## 1.0.0
- Initial Release

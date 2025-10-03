---
"kafka-connect-plugins": patch
---

A bunch of changes from the Culture Amp Kotlin library template - https://github.com/cultureamp/kotlin-composite-library-template/ - with the main aim of simplifying the release process, but other easy wins along the way

- Changesets support
- github action to validate backstage files
- remove the owasp dependency check - it doesn't work any more. The snyk github actions already appear to be set up
- Use foojay resolver + gradle toolchains so it can automatically download the correct version of java
- Add devbox support :-D
- Bump Kotlin version to the also-but-less outdated 1.9.22 (came from the template)
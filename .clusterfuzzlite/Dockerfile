# gcr.io/oss-fuzz-base/base-builder-jvm:ubuntu-24-04
FROM gcr.io/oss-fuzz-base/base-builder-jvm@sha256:7bbc26fb22a04a2d32ccae43dde3cecd582cf25faf41ead13fdcd9f9a2899dd3

# Ubuntu ships Maven 3.8.7, below the 3.8.9 that spotbugs-maven-plugin declares
# as a prerequisite; Maven resolves plugin descriptors before it honours
# `-Dspotbugs.skip`, so even a skipped plugin has to be loadable. The Apache
# archive is used rather than a mirror because mirrors drop old releases.
ARG MAVEN_VERSION=3.9.16
ARG MAVEN_SHA512=831a8591fe20c8243b1dbe7d71e3244f31d1665b0804b2e825e38cbbe5ce0cafb8338851f90780735568773e0a6cd07bbec107cda0b896b008b861075358b6f6

RUN set -eux \
 && tarball="apache-maven-$MAVEN_VERSION-bin.tar.gz" \
 && curl -fsSLO --retry 3 --retry-connrefused \
      "https://archive.apache.org/dist/maven/maven-3/$MAVEN_VERSION/binaries/$tarball" \
 && echo "$MAVEN_SHA512  $tarball" | sha512sum --check --strict - \
 && mkdir -p /opt/maven \
 && tar -xzf "$tarball" -C /opt/maven --strip-components=1 \
 && rm "$tarball" \
 && ln -s /opt/maven/bin/mvn /usr/local/bin/mvn \
 && mvn --version

COPY . $SRC/blazingmq-sdk-java
COPY .clusterfuzzlite/build.sh $SRC/

WORKDIR $SRC/blazingmq-sdk-java

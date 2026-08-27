#!/bin/bash -eu
# Copyright 2026 Bloomberg Finance L.P.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FUZZERS_DIR=bmq-sdk/src/test/java/com/bloomberg/bmq/fuzz
FUZZERS_PACKAGE=com.bloomberg.bmq.fuzz

MVN_FLAGS=(
  --batch-mode
  -Dmaven.test.skip=true
  -Dspotbugs.skip=true
  -Dspotless.check.skip=true
  -Dmaven.javadoc.skip=true
  -Dsource.skip=true
  -Djacoco.skip=true
)

mvn "${MVN_FLAGS[@]}" -pl bmq-sdk package
mvn "${MVN_FLAGS[@]}" -pl bmq-sdk dependency:copy-dependencies \
  -DincludeScope=runtime -DoutputDirectory="$OUT"

find bmq-sdk/target -maxdepth 1 -name 'bmq-sdk-*.jar' \
  ! -name '*-sources.jar' ! -name '*-javadoc.jar' -exec cp {} "$OUT/" \;

# The fuzzers live in the test tree, but pulling in the whole test scope just
# to compile two files is not worth it: javac against the artifacts already
# staged in $OUT plus the Jazzer API is enough.
mkdir -p fuzzer-classes
javac -cp "$JAZZER_API_PATH:$(echo "$OUT"/*.jar | tr ' ' ':')" \
  -d fuzzer-classes "$FUZZERS_DIR"/*.java
(cd fuzzer-classes && jar cf "$OUT/bmq-fuzzers.jar" .)

for fuzzer_source in "$FUZZERS_DIR"/*Fuzzer.java; do
  fuzzer=$(basename "$fuzzer_source" .java)
  cat > "$OUT/$fuzzer" <<EOF
#!/bin/bash
# LLVMFuzzerTestOneInput: the string OSS-Fuzz greps for to tell a fuzz target
# apart from the other files staged in \$OUT.
this_dir=\$(dirname "\$0")
LD_LIBRARY_PATH="\$JVM_LD_LIBRARY_PATH":\$this_dir \\
\$this_dir/jazzer_driver \\
  --agent_path=\$this_dir/jazzer_agent_deploy.jar \\
  --cp=\$(echo \$this_dir/*.jar | tr ' ' ':') \\
  --target_class=$FUZZERS_PACKAGE.$fuzzer \\
  --instrumentation_includes='com.bloomberg.**' \\
  --jvm_args="-Xmx2048m:-Djava.awt.headless=true:-ea" \\
  "\$@"
EOF
  chmod +x "$OUT/$fuzzer"
done

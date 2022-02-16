FROM ubuntu:22.04

# CASCADING_RELEASE is the release tag for cascading to compile (ex. https://github.com/cwensel/cascading/releases/tag/4.5-wip-10)
# SCALDING_CASCADING_VERSION is the arg scalding uses to resolve the cascading version
ENV CASCADING_RELEASE 4.5-wip-10
ENV SCALDING_CASCADING_VERSION 4.5.0-wip-dev

COPY "./download-java.sh" "/scalding/experimental-cascading/docker/download-java.sh"
RUN "/scalding/experimental-cascading/docker/download-java.sh"

COPY "./compile-cascading.sh" "/scalding/experimental-cascading/docker/compile-cascading.sh"
RUN "/scalding/experimental-cascading/docker/compile-cascading.sh"

COPY "./cleanup-image.sh" "/scalding/experimental-cascading/docker/cleanup-image.sh"
RUN "/scalding/experimental-cascading/docker/cleanup-image.sh"

COPY "./compile-scalding.sh" "/scalding/experimental-cascading/docker/compile-scalding.sh"
ENTRYPOINT ["/bin/bash", "/scalding/experimental-cascading/docker/compile-scalding.sh"]

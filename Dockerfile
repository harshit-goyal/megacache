FROM python:3.12-alpine

WORKDIR /app
COPY pyproject.toml README.md LICENSE ./
COPY src ./src
RUN pip install --no-cache-dir . \
    && mkdir -p /var/lib/megacache \
    && chown 65532:65532 /var/lib/megacache

ENV MEGACACHE_EVENT_STATE_FILE=/var/lib/megacache/events-state.json
ENV MEGACACHE_CONTROL_STATE_DIRECTORY=/var/lib/megacache/control
USER 65532:65532
EXPOSE 8080 6380
ENTRYPOINT ["mc"]

FROM python:3.12-alpine

WORKDIR /app
COPY pyproject.toml README.md LICENSE ./
COPY src ./src
RUN pip install --no-cache-dir .

USER 65532:65532
EXPOSE 8080 6380
ENTRYPOINT ["mc"]

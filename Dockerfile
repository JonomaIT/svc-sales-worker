# Three stages: dependencies (hash-checked), tests (the build fails if they fail) and the runtime
# image, which carries only the code and its dependencies: no pip, no tests, non-root user.

FROM python:3.15-slim AS deps
COPY requirements.txt .
RUN pip install --no-cache-dir --require-hashes --target /deps -r requirements.txt

FROM python:3.15-slim AS test
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=/deps
COPY --from=deps /deps /deps
WORKDIR /app
COPY src/ src/
COPY tests/ tests/
RUN python -m unittest discover -s tests/unit -v && touch /tests-passed

FROM python:3.15-slim
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/deps \
    PORT=8080

# The app never installs anything at runtime: pip (and what it bundles) only adds attack surface.
RUN python -m pip uninstall -y pip setuptools wheel >/dev/null 2>&1; \
    rm -rf /usr/local/lib/python3*/site-packages/pip* /usr/local/lib/python3*/site-packages/setuptools* \
    && useradd --system --uid 10001 app

COPY --from=deps /deps /deps
# Ties this stage to the tests: BuildKit skips stages nothing depends on.
COPY --from=test /tests-passed /tmp/tests-passed
WORKDIR /app
COPY src/worker.py .

USER 10001
EXPOSE 8080
CMD ["python", "worker.py"]

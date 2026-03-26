FROM python:3.13-slim

WORKDIR /app

# git is required by gitpython at runtime
RUN apt-get update && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

# Install uv for fast, reproducible dependency resolution
COPY --from=ghcr.io/astral-sh/uv:0.11.1 /uv /usr/local/bin/uv

# Install dependencies into the system Python (not a venv) so the
# plain `python` entrypoint can import them regardless of working directory.
COPY pyproject.toml uv.lock ./
RUN uv export --no-dev --format requirements-txt > /tmp/requirements.txt \
    && pip install --no-cache-dir -r /tmp/requirements.txt

# Copy source last to maximize Docker layer cache reuse
COPY src/ ./src/

# Add src/ to PYTHONPATH so all module imports resolve correctly
# when GitHub Actions sets the working directory to the checked-out workspace.
ENV PYTHONPATH=/app/src

# GitHub Actions mounts the workspace as a different owner than the container user.
# Mark it as safe so gitpython can read the repository without "dubious ownership" errors.
# Use --system (writes to /etc/gitconfig) so it is honoured regardless of $HOME override.
RUN git config --system --add safe.directory /github/workspace

ENTRYPOINT ["python", "/app/src/main.py"]

FROM node:22-slim

# Install git
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y git gnupg2


# Install UV and python
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
RUN uv python install 3.12

# Install Claude Code
RUN npm install -g @anthropic-ai/claude-code


#!/bin/bash

# Install dependencies
uv sync --all-extras

# Activate virtual environment created by uv
source .venv/bin/activate

# Install pre-commit hooks
pre-commit install

# Write to .claude/settings.json to enable all MCP servers:
echo '{"enableAllProjectMcpServers": true}' > ~/.claude/settings.local.json

# Add to trusted Claude Project
# Get current working directory
CURRENT_DIR=$(pwd)

# Create the project configuration
PROJECT_CONFIG='{
  "allowedTools": [],
  "history": [],
  "dontCrawlDirectory": false,
  "mcpContextUris": [],
  "mcpServers": {},
  "enabledMcpjsonServers": [],
  "disabledMcpjsonServers": [],
  "hasTrustDialogAccepted": true,
  "projectOnboardingSeenCount": 0,
  "hasClaudeMdExternalIncludesApproved": false,
  "hasClaudeMdExternalIncludesWarningShown": false,
  "exampleFiles": [],
  "exampleFilesGeneratedAt": 0,
  "hasCompletedProjectOnboarding": true
}'

# Add project configuration to .claude.json
jq --arg dir "$CURRENT_DIR" --argjson config "$PROJECT_CONFIG" '.project[$dir] = $config' ~/host.claude.json > ~/.claude.json


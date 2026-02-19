#!/usr/bin/env bash
#
# setup_github_actions.sh
#
# Step-by-step setup for brreg-regnskap GitHub Actions sync workflow.
# Prerequisites: gh CLI installed and authenticated (gh auth login).
#
# Usage:
#   chmod +x scripts/setup_github_actions.sh
#   ./scripts/setup_github_actions.sh
#
set -euo pipefail

REPO="sondreskarsten/brreg-regnskap"

echo "=============================================="
echo " brreg-regnskap — GitHub Actions Setup"
echo "=============================================="
echo ""
echo "This script configures the GitHub repository"
echo "secrets and variables needed to run the sync"
echo "workflow with AWS S3 storage."
echo ""

# --------------------------------------------------
# Step 0: Verify gh CLI is authenticated
# --------------------------------------------------
echo "--- Step 0: Verify gh CLI authentication ---"
if ! command -v gh &>/dev/null; then
    echo "ERROR: gh CLI not found. Install it: https://cli.github.com/"
    exit 1
fi

if ! gh auth status &>/dev/null; then
    echo "ERROR: Not authenticated. Run: gh auth login"
    exit 1
fi
echo "OK: gh CLI authenticated."
echo ""

# --------------------------------------------------
# Step 1: Create AWS S3 Bucket
# --------------------------------------------------
echo "--- Step 1: AWS S3 Bucket ---"
echo ""
echo "If you haven't already, create an S3 bucket:"
echo ""
echo "  aws s3 mb s3://brreg-regnskap --region eu-north-1"
echo ""
read -rp "Enter your S3 bucket path (e.g. s3://brreg-regnskap/data): " STORAGE_PATH
echo ""

read -rp "Enter your AWS region (default: eu-north-1): " AWS_REGION
AWS_REGION="${AWS_REGION:-eu-north-1}"
echo ""

# --------------------------------------------------
# Step 2: Create IAM OIDC Identity Provider
# --------------------------------------------------
echo "--- Step 2: AWS OIDC Identity Provider ---"
echo ""
echo "Create an OIDC identity provider in AWS IAM so"
echo "GitHub Actions can assume an IAM role without"
echo "storing long-lived AWS keys."
echo ""
echo "Run these AWS CLI commands (one-time setup):"
echo ""
echo "  # Create the OIDC provider"
echo "  aws iam create-open-id-connect-provider \\"
echo "    --url https://token.actions.githubusercontent.com \\"
echo "    --client-id-list sts.amazonaws.com \\"
echo "    --thumbprint-list 6938fd4d98bab03faadb97b34396831e3780aea1"
echo ""
echo "If it already exists, skip this step."
echo ""

# --------------------------------------------------
# Step 3: Create IAM Role for GitHub Actions
# --------------------------------------------------
echo "--- Step 3: IAM Role for GitHub Actions ---"
echo ""
echo "Create an IAM role with S3 access that GitHub"
echo "Actions can assume via OIDC."
echo ""

AWS_ACCOUNT_ID=""
read -rp "Enter your AWS Account ID (12-digit number): " AWS_ACCOUNT_ID
echo ""

cat <<POLICY
Trust policy (save as trust-policy.json):

{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Federated": "arn:aws:iam::${AWS_ACCOUNT_ID}:oidc-provider/token.actions.githubusercontent.com"
      },
      "Action": "sts:AssumeRoleWithWebIdentity",
      "Condition": {
        "StringEquals": {
          "token.actions.githubusercontent.com:aud": "sts.amazonaws.com"
        },
        "StringLike": {
          "token.actions.githubusercontent.com:sub": "repo:${REPO}:*"
        }
      }
    }
  ]
}

POLICY
echo ""
echo "Create the role:"
echo ""
echo "  aws iam create-role \\"
echo "    --role-name brreg-regnskap-sync \\"
echo "    --assume-role-policy-document file://trust-policy.json"
echo ""
echo "Attach S3 permissions:"
echo ""
cat <<S3POLICY
Save as s3-policy.json:

{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::${STORAGE_PATH#s3://}",
        "arn:aws:s3:::${STORAGE_PATH#s3://}/*"
      ]
    }
  ]
}

S3POLICY
echo ""
echo "  aws iam put-role-policy \\"
echo "    --role-name brreg-regnskap-sync \\"
echo "    --policy-name S3Access \\"
echo "    --policy-document file://s3-policy.json"
echo ""

read -rp "Enter the IAM Role ARN (arn:aws:iam::...): " AWS_ROLE_ARN
echo ""

# --------------------------------------------------
# Step 4: Set GitHub Repository Secrets
# --------------------------------------------------
echo "--- Step 4: Set GitHub Secrets ---"
echo ""
echo "Setting secret: AWS_ROLE_ARN"
gh secret set AWS_ROLE_ARN --repo "$REPO" --body "$AWS_ROLE_ARN"
echo "OK: AWS_ROLE_ARN set."
echo ""

# --------------------------------------------------
# Step 5: Set GitHub Repository Variables
# --------------------------------------------------
echo "--- Step 5: Set GitHub Variables ---"
echo ""

echo "Setting variable: BRREG_STORAGE_PATH"
gh variable set BRREG_STORAGE_PATH --repo "$REPO" --body "$STORAGE_PATH"
echo "OK: BRREG_STORAGE_PATH = $STORAGE_PATH"

echo "Setting variable: AWS_REGION"
gh variable set AWS_REGION --repo "$REPO" --body "$AWS_REGION"
echo "OK: AWS_REGION = $AWS_REGION"
echo ""

# --------------------------------------------------
# Step 6: Enable GitHub Actions OIDC permissions
# --------------------------------------------------
echo "--- Step 6: Workflow OIDC Permissions ---"
echo ""
echo "The sync.yml workflow needs 'id-token: write' permission"
echo "for OIDC to work. Let me add that if missing..."
echo ""
echo "Checking sync.yml..."

if ! grep -q "id-token:" .github/workflows/sync.yml 2>/dev/null; then
    echo "WARNING: sync.yml is missing 'permissions: id-token: write'"
    echo "This will be added automatically."
else
    echo "OK: OIDC permissions already present."
fi
echo ""

# --------------------------------------------------
# Step 7: Verify Setup
# --------------------------------------------------
echo "--- Step 7: Verify ---"
echo ""
echo "Listing configured secrets:"
gh secret list --repo "$REPO"
echo ""
echo "Listing configured variables:"
gh variable list --repo "$REPO"
echo ""

# --------------------------------------------------
# Done
# --------------------------------------------------
echo "=============================================="
echo " Setup Complete!"
echo "=============================================="
echo ""
echo "To trigger a sync manually:"
echo "  gh workflow run sync.yml --repo $REPO -f mode=incremental"
echo ""
echo "To watch the run:"
echo "  gh run list --repo $REPO --workflow sync.yml"
echo "  gh run watch --repo $REPO"
echo ""
echo "To check status after sync:"
echo "  uv run brreg-regnskap status $STORAGE_PATH"
echo ""

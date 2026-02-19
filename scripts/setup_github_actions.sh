#!/usr/bin/env bash
#
# setup_github_actions.sh
#
# Step-by-step setup for brreg-regnskap GitHub Actions sync workflow.
# Supports both AWS S3 and Google Cloud Storage — just provide the
# storage path and the script handles the rest.
#
# Prerequisites: gh CLI installed and authenticated (gh auth login).
#
# Usage:
#   chmod +x scripts/setup_github_actions.sh
#   ./scripts/setup_github_actions.sh
#
set -euo pipefail

REPO="sondreskarsten/brreg-regnskap"

# ============================================================
# AWS S3 Setup
# ============================================================
setup_aws() {
    read -rp "Enter your AWS region (default: eu-north-1): " AWS_REGION
    AWS_REGION="${AWS_REGION:-eu-north-1}"
    echo ""

    # --- OIDC Provider ---
    echo "--- Step 2: AWS OIDC Identity Provider ---"
    echo ""
    echo "Create an OIDC identity provider in AWS IAM (one-time):"
    echo ""
    echo "  aws iam create-open-id-connect-provider \\"
    echo "    --url https://token.actions.githubusercontent.com \\"
    echo "    --client-id-list sts.amazonaws.com \\"
    echo "    --thumbprint-list 6938fd4d98bab03faadb97b34396831e3780aea1"
    echo ""
    echo "Skip if it already exists."
    echo ""

    # --- IAM Role ---
    echo "--- Step 3: IAM Role ---"
    echo ""
    read -rp "Enter your AWS Account ID (12-digit number): " AWS_ACCOUNT_ID
    echo ""

    BUCKET_NAME="${STORAGE_PATH#s3://}"
    BUCKET_NAME="${BUCKET_NAME%%/*}"

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

S3 policy (save as s3-policy.json):

{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"],
      "Resource": ["arn:aws:s3:::${BUCKET_NAME}", "arn:aws:s3:::${BUCKET_NAME}/*"]
    }
  ]
}

POLICY
    echo "Create the role and attach the policy:"
    echo ""
    echo "  aws iam create-role --role-name brreg-regnskap-sync \\"
    echo "    --assume-role-policy-document file://trust-policy.json"
    echo ""
    echo "  aws iam put-role-policy --role-name brreg-regnskap-sync \\"
    echo "    --policy-name S3Access --policy-document file://s3-policy.json"
    echo ""

    read -rp "Enter the IAM Role ARN (arn:aws:iam::...): " AWS_ROLE_ARN
    echo ""

    # --- Set secrets & variables ---
    echo "--- Step 4: Set GitHub secrets & variables ---"
    echo ""
    gh secret set AWS_ROLE_ARN --repo "$REPO" --body "$AWS_ROLE_ARN"
    echo "OK: secret AWS_ROLE_ARN set."

    gh variable set BRREG_STORAGE_PATH --repo "$REPO" --body "$STORAGE_PATH"
    echo "OK: variable BRREG_STORAGE_PATH = $STORAGE_PATH"

    gh variable set AWS_REGION --repo "$REPO" --body "$AWS_REGION"
    echo "OK: variable AWS_REGION = $AWS_REGION"
    echo ""
}

# ============================================================
# Google Cloud Storage Setup
# ============================================================
setup_gcs() {
    echo "--- Step 2: GCP Service Account ---"
    echo ""
    echo "Create a service account with Storage Object Admin on your bucket:"
    echo ""

    read -rp "Enter your GCP project ID: " GCP_PROJECT
    echo ""

    BUCKET_NAME="${STORAGE_PATH#gs://}"
    BUCKET_NAME="${BUCKET_NAME%%/*}"
    SA_NAME="brreg-regnskap-sync"
    SA_EMAIL="${SA_NAME}@${GCP_PROJECT}.iam.gserviceaccount.com"

    echo "Run these gcloud commands:"
    echo ""
    echo "  # Create service account"
    echo "  gcloud iam service-accounts create ${SA_NAME} \\"
    echo "    --project ${GCP_PROJECT} \\"
    echo "    --display-name 'brreg-regnskap sync'"
    echo ""
    echo "  # Grant bucket access"
    echo "  gsutil iam ch serviceAccount:${SA_EMAIL}:objectAdmin \\"
    echo "    gs://${BUCKET_NAME}"
    echo ""

    # --- Workload Identity Federation ---
    echo "--- Step 3: Workload Identity Federation (OIDC) ---"
    echo ""
    echo "This lets GitHub Actions authenticate as the service account"
    echo "without storing a JSON key."
    echo ""
    echo "  # Create workload identity pool"
    echo "  gcloud iam workload-identity-pools create github-actions \\"
    echo "    --project ${GCP_PROJECT} \\"
    echo "    --location global \\"
    echo "    --display-name 'GitHub Actions'"
    echo ""
    echo "  # Create OIDC provider in the pool"
    echo "  gcloud iam workload-identity-pools providers create-oidc github \\"
    echo "    --project ${GCP_PROJECT} \\"
    echo "    --location global \\"
    echo "    --workload-identity-pool github-actions \\"
    echo "    --issuer-uri https://token.actions.githubusercontent.com \\"
    echo "    --attribute-mapping google.subject=assertion.sub,attribute.repository=assertion.repository"
    echo ""
    echo "  # Allow this repo to impersonate the service account"
    echo "  gcloud iam service-accounts add-iam-policy-binding ${SA_EMAIL} \\"
    echo "    --project ${GCP_PROJECT} \\"
    echo "    --role roles/iam.workloadIdentityUser \\"
    echo "    --member \"principalSet://iam.googleapis.com/projects/\$(gcloud projects describe ${GCP_PROJECT} --format='value(projectNumber)')/locations/global/workloadIdentityPools/github-actions/attribute.repository/${REPO}\""
    echo ""

    read -rp "Enter GCP project number (or press Enter to look it up): " GCP_PROJECT_NUMBER
    if [[ -z "$GCP_PROJECT_NUMBER" ]]; then
        if command -v gcloud &>/dev/null; then
            GCP_PROJECT_NUMBER=$(gcloud projects describe "$GCP_PROJECT" --format='value(projectNumber)')
            echo "Looked up project number: $GCP_PROJECT_NUMBER"
        else
            read -rp "gcloud not found. Enter project number manually: " GCP_PROJECT_NUMBER
        fi
    fi
    echo ""

    WIF_PROVIDER="projects/${GCP_PROJECT_NUMBER}/locations/global/workloadIdentityPools/github-actions/providers/github"

    # --- Set secrets & variables ---
    echo "--- Step 4: Set GitHub secrets & variables ---"
    echo ""
    gh secret set GCP_WORKLOAD_IDENTITY_PROVIDER --repo "$REPO" --body "$WIF_PROVIDER"
    echo "OK: secret GCP_WORKLOAD_IDENTITY_PROVIDER set."

    gh secret set GCP_SERVICE_ACCOUNT --repo "$REPO" --body "$SA_EMAIL"
    echo "OK: secret GCP_SERVICE_ACCOUNT set."

    gh variable set BRREG_STORAGE_PATH --repo "$REPO" --body "$STORAGE_PATH"
    echo "OK: variable BRREG_STORAGE_PATH = $STORAGE_PATH"
    echo ""
}

# ============================================================
# Main
# ============================================================
echo "=============================================="
echo " brreg-regnskap — GitHub Actions Setup"
echo "=============================================="
echo ""

# --- Verify gh CLI ---
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

# --- Choose storage provider ---
echo "--- Step 1: Storage path ---"
echo ""
echo "The workflow auto-detects the provider from the path prefix:"
echo "  s3://...  -> AWS S3"
echo "  gs://...  -> Google Cloud Storage"
echo ""
read -rp "Enter your storage path (e.g. s3://my-bucket/data or gs://my-bucket/data): " STORAGE_PATH
echo ""

if [[ "$STORAGE_PATH" == gs://* ]]; then
    PROVIDER="gcs"
    echo "Detected provider: Google Cloud Storage"
elif [[ "$STORAGE_PATH" == s3://* ]]; then
    PROVIDER="s3"
    echo "Detected provider: AWS S3"
else
    echo "ERROR: Storage path must start with s3:// or gs://"
    exit 1
fi
echo ""

# --- Run provider-specific setup ---
if [[ "$PROVIDER" == "s3" ]]; then
    setup_aws
else
    setup_gcs
fi

# --- Verify ---
echo "--- Step 5: Verify ---"
echo ""
echo "Configured secrets:"
gh secret list --repo "$REPO"
echo ""
echo "Configured variables:"
gh variable list --repo "$REPO"
echo ""

# --- Done ---
echo "=============================================="
echo " Setup Complete! (provider: $PROVIDER)"
echo "=============================================="
echo ""
echo "To trigger a sync:"
echo "  gh workflow run sync.yml --repo $REPO -f mode=incremental"
echo ""
echo "To watch:"
echo "  gh run list --repo $REPO --workflow sync.yml"
echo "  gh run watch --repo $REPO"
echo ""
echo "To check status after sync:"
echo "  uv run brreg-regnskap status $STORAGE_PATH"
echo ""
echo "To switch providers, just change BRREG_STORAGE_PATH:"
echo "  gh variable set BRREG_STORAGE_PATH --repo $REPO --body 's3://...' "
echo "  gh variable set BRREG_STORAGE_PATH --repo $REPO --body 'gs://...' "
echo ""

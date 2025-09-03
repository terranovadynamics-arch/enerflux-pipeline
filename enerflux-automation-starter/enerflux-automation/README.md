# Enerflux Automation Starter

## What this does
- Daily: fetch WTI spot (FRED DCOILWTICO) -> upload to R2 as WTI-DAILY/latest + dated file.
- Weekly (Monday): build ENERGY-WEEKLY CSV/PDF from daily series -> upload to R2 as ENERGY-WEEKLY/latest + dated files.

## Setup
1. New private GitHub repo; upload these files (or unzip the bundle).
2. Repo Secrets (Settings -> Secrets and variables -> Actions):
   - R2_ACCOUNT_ID
   - R2_BUCKET (e.g. enerflux-prod)
   - R2_ACCESS_KEY_ID
   - R2_SECRET_ACCESS_KEY
3. Actions will run on schedule.

## Manual trigger
- GitHub -> Actions -> Enerflux Data Machine -> Run workflow

Verify:
- https://enerflux.org/latest/WTI-DAILY
- https://enerflux.org/latest/ENERGY-WEEKLY

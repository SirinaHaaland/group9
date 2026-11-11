# Project — CI/CD Pipelines: Dev/Prod with GitHub Actions

## Learning Objectives

- Understand Dev/Prod environment separation
- Configure GitHub Environments with approval gates
- Work with the dev/main branch strategy from VS Code
- Execute PySpark data pipelines on a self-hosted runner

---

## What Do "Dev" and "Prod" Mean?

If you haven't worked on a team software project before, these terms might be new:

- **Dev (development)**: a "sandbox" environment where you try out changes. Code here is
  expected to be unfinished or break sometimes — that's fine, it's isolated from anything
  anyone else relies on. In this repo, the `dev` branch and the `dat535-dev.yml` workflow
  write to a separate scratch folder (`/tmp/spark-lab-data/dev/`) so a bad run never
  touches real data.
- **Prod (production)**: the environment that represents the "real," trusted version of
  the project — the one other people or systems depend on. Changes only reach prod after
  they've been reviewed and merged, which is why it's protected by a manual approval step
  before it runs. Here, that's the `main` branch and the `dat535-prod.yml` workflow, writing
  to `/tmp/spark-lab-data/prod/`.

The general idea, common in industry: **experiment in dev, get changes reviewed, then
promote them to prod.** GitHub Actions lets us automate and enforce this with separate
workflows per branch and "Environments" that can require human approval before a job runs.

---

## The Dev/Prod Model

1. Work on `dev` branch locally or on the cluster
2. Push to `dev` — [.github/workflows/dat535-dev.yml](.github/workflows/dat535-dev.yml) runs automatically on the self-hosted runner
3. Open a Pull Request from `dev` to `main` to release
4. After merge, [.github/workflows/dat535-prod.yml](.github/workflows/dat535-prod.yml) runs on the `main` environment and requires manual deployment approval

---

## Part A: Configure GitHub Environments

### 1. Create the `dev` environment

1. Go to your GitHub repo → **Settings** → **Environments** → **New environment**
2. Name it `dev` → **Configure environment**
3. Leave "Deployment protection rules" empty (dev auto-deploys on every push)

### 2. Create the `prod` environment (`main`)

1. **New environment** → name it `main`
2. Under **Deployment protection rules** → enable **Required reviewers**
3. Add yourself and/or group partners as reviewers
4. Save — now every production deployment requires a manual approval click in GitHub Actions

---

## Part B: Branch Strategy

```bash
# Day-to-day development (dev branch) 
git checkout -b dev

# work in VSCode
git add .
# if you have made changes in lab2
git commit -m "feat(lab2): implement ingest"
git push origin dev
```

1. GitHub Actions dev pipeline starts automatically
2. Watch it at: github.com/YourGitHubUsername/groupXX-dat535

### Release to production

1. Open a Pull Request on GitHub: dev → main
2. Reviewer reviews the diff and approves
3. Merge the PR
    → GitHub Actions production pipeline starts
    → Reviewer is asked to approve the deployment
    → After approval, prod pipeline runs

Open a PR `dev` → `main`, request review, merge when ready.

---

## Viewing Results

### GitHub Actions tab in the browser

github.com/YourGitHubUsername/groupXX-dat535 → Actions tab → select a run → view live logs.

### GitHub Actions VSCode Extension

With the GitHub Actions extension installed:

Click the GitHub Actions icon in the Activity Bar
See all workflow runs and their status
Click a run to view logs inline in VSCode

---

## How the Pipeline Code is Environment-Aware

Both Dev and Production pipelines run on self-hosted runners where Apache Spark and Java are pre-installed.

Execution commands in GitHub Actions workflows:

```bash
# Activate Spark Python environment
source ~/spark-env/bin/activate

# Execute all pipeline labs
python $GITHUB_WORKSPACE/run_pipeline.py all
```

---

## Hands-On Exercise

1. Create `dev` branch and push a trivial change to trigger the dev pipeline

    ```bash
    git checkout -b dev
    git push -u origin dev
    ```

    Add a log statement or update a comment in `lab2_pipeline.py`.
    Commit and push to dev — watch the dev pipeline run on the Actions tab.

2. Open a PR and merge to `main` to trigger the prod pipeline (approval required)

    Watch the prod pipeline — approve the deployment

---

## Checklist

- [ ] dev GitHub Environment created (no protection rules)
- [ ] prod GitHub Environment created (with required reviewer)
- [ ] Workflow files exist at .github/workflows/dat535-dev.yml and dat535-prod.yml
- [ ] Pushed to dev branch — pipeline succeeded
- [ ] Opened PR, merged to main — prod pipeline ran with approval
- [ ] Downloaded and opened artifacts in VSCode

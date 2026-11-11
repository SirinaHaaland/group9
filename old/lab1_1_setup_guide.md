# Lab 1.1 — Technical Setup

## Learning Objectives

- Connect VSCode to the OpenStack cluster via Remote-SSH
- Run your first PySpark session inside a notebook

---

## Part A: Install Required VSCode Extensions

Install from the Extensions panel :

| Extension | ID |
| ----------- | ---- |
| Python | `ms-python.python` |
| Jupyter | `ms-toolsai.jupyter` |
| Remote - SSH | `ms-vscode-remote.remote-ssh` |
| GitLens | `eamodio.gitlens` |
| GitHub Actions | `github.vscode-github-actions` |

---

## Part B: OpenStack Cluster Connection

1. Login in to [http://5gnuc1.ux.uis.no/horizon](http://5gnuc1.ux.uis.no/horizon) using the details provides to your group. [Works only from UiS network, login details are provided Lab session in first weeks lab]

2. Go to Project->Network->Network [You will perform this step only once]

    - Manage Security Group Rules:
        Project -> Network -> Security Groups -> Manage Rules ->Add Rule -> Rule (select SSH) , CIDR (choose IP from where you want to connect from) (Ex: 152.94.0.0/16 , open whole university network) -> Add.

        (If you don’t have this rule, you will not be able to SSH to the VM created)
        You don’t have to add it, if this rule already exists. However, this is how you open other required ports.

3. Create a test/ Spark VM: Go to Project -> Compute -> Instances

    - Launch -> Select Name -> Source (Ubuntu) -> Flavor (m1.large ) -> -> -> (Key Pair – Only first time, create your ssh key or upload existing ssh key).
    - Creating floating ip (first time only): Project -> Compute -> Instance -> options from instance --> associate floating ip -> create IP -> allocate IP .
    - To SSH to VM use (if Ubuntu image ) user name : ubuntu --->
    - ssh username@floating_ip -i ssh_key

4. Copy the from your local machine to VM.

    - scp lab1_install-spark.sh ubuntu@floating_IP:/home/ubuntu/

5. Now go back to the VM and Change the permissions on the file and start the installation of spark.
    - chmod +x lab1_install-spark.sh
    - ./lab1_install-spark.sh

6. Script installation will take anywhere between 15mins to 45mins.

7. Check the services are running or start the services.

```bash
# Start services (runs in background)"
$ ~/start-spark-notebook.sh
$ ~/start-spark-standalone.sh

# Check status
$ ~/spark-status.sh

# Stop all services
$ ~/stop-spark.sh
```

---

## Part C: Clone the Course Repository

1. Create a New Empty Repository

    - Log into your profile on GitHub.
    - Create a new repository `github.com/YourGitHubUsername/groupXX-dat535`. Important: Do not add a README, .gitignore, or license. Leave it completely empty.

2. Run the Git Commands

```bash
# Clone the course repository to your computer
git clone https://github.com/jayachanders/dat535.git

# Move inside the downloaded directory
cd dat535

# Rename the original source remote link to 'upstream'
git remote rename origin upstream

# Link your new empty online repository as the main 'origin'
git remote add origin https://github.com/YourGitHubUsername/groupXX-dat535

# Push all code and commit history to your new repository
git push -u origin main

```

---

## Checklist

- [ ] Python 3.11+ installed
- [ ] Java 8 installed (`java -version`)
- [ ] `pyspark` installed and importable
- [ ] VSCode extensions installed
- [ ] Connected to cluster via Remote-SSH
- [ ] Repository cloned

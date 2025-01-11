---
title: Fraud Detection Airflow
emoji: 🏆
colorFrom: green
colorTo: purple
sdk: docker
pinned: false
license: apache-2.0
short_description: Fraud Detection Data Pipeline Flow
---

Check out the configuration reference at https://huggingface.co/docs/hub/spaces-config-reference



Step 1: Create a Docker Container in Hugging Face Space
Navigate to the Hugging Face Spaces.
Create a new Space and select Docker as the runtime environment.
Configure the basic details such as the project name and visibility (public or private).

Step 2: Push Code to Hugging Face Space
Clone the repository for your Space:

Copy code
$ git clone <your-space-url>
Copy your project files into the cloned repository directory.
Commit and push the files to the Space repository:

$ git add .
$ git commit -m "Initial project setup"
$ git push <remotereponame> <branch>

Step 3: Set Variables and Secrets in Settings
Go to the Settings tab of your Space on Hugging Face.
Add necessary environment variables and secrets such as API keys or database credentials.
Example:
API_KEY=<your-api-key>
DATABASE_URL=<your-database-url>
SERVER_SECRET=<sshkey>
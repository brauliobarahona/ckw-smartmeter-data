<a className="gh-badge" href="https://datahub.io/@brauliobarahona/CKW_smart_meter_data?_gl=1*bvkdbm*_ga*MTI5ODg4NTcwNy4xNzU4NzI0Mjg5*_ga_R6X92HM43Q*czE3NTkxNzc1NDUkbzEwJGcxJHQxNzU5MTc3ODg3JGo2MCRsMCRoMA.."><img src="https://badgen.net/badge/icon/View%20on%20datahub.io/orange?icon=https://datahub.io/datahub-cube-badge-icon.svg&label&scale=1.25" alt="badge" /></a>

This data set contains a small sample of the CKW data set A sorted per smart meter ID, stored as parquet files named with the id field of the corresponding smart meter anonymised data. Example: 027ceb7b8fd77a4b11b3b497e9f0b174.parquet

## Data

**!**Clean up and complete datapackage.json
**!**Create a sample dataset in csv format for visualization

### Description

### Sources

## Quick start

**!**TODO: simple clone and run a scrip or use the cli

## Setup remote environment

These are the steps applied to setup the working environment and then parse the CKW in a virtual machine (VM) hosted in Switch engines for the AISOP project. First, setup workflow to work remotely from VM.

In order to authenticate to git and be able to clone the repository, first make sure you have
- [ ] a github account
- [ ] a personal access token
- [ ] a ssh key pair
- [ ] a ssh key pair added to your github account
- [ ] a ssh key pair added to your ssh-agent

1. Create a new ssh key pair
```bash
ssh-keygen -t rsa -b 4096 -C "hintisberg"
ssh-keygen -t ecdsa -C "hintisberg in braulio's github account"
````

2. Copy the public key to your clipboard

```bash
pbcopy < ~/.ssh/id_rsa.pub

# or with cat and pipe to pbcopy
cat ~/.ssh/id_rsa.pub | pbcopy

# or copy to cliboard without pbcopy
cat ~/.ssh/id_rsa.pub
```

3. Add SSH key to your github account

4. Set SSH agent to remembner your key

```bash
# Start the ssh-agent in the background
eval "$(ssh-agent -s)"

# Add your specific key (adjust the filename if you used ecdsa)
ssh-add ~/.ssh/id_rsa
```

## TODOs

**!**TODO: rename to ckw_data_parser
**!**TODO: debug/test : scripts/ckw_download.py --list-only :: prints file tables for both datasets
**!**TODO: debug/test : scripts/ckw_download.py --dataset a :: shows dataset A files and prompts for selection
**!**TODO: test download + decompress cycle


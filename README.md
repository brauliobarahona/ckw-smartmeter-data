This data set contains a small sample of the CKW data set A sorted per smart meter ID, stored as parquet files named with the id field of the corresponding smart meter anonymised data. Example: 027ceb7b8fd77a4b11b3b497e9f0b174.parquet

## Data

### Description

### Sources

## Preparation

### Processing

Setup workflow to work remotely from VM hosted in Switch engines.

In order to authenticate to git and be able to clone the repository, first make sure you have
- [ ] a github account
- [ ] a personal access token
- [ ] a ssh key pair
- [ ] a ssh key pair added to your github account
- [ ] a ssh key pair added to your ssh-agent

1. create a new ssh key pair
```bash
ssh-keygen -t rsa -b 4096 -C "hintisberg"
ssh-keygen -t ecdsa -C "hintisberg in braulio's github account"
````

copy the public key to your clipboard

```bash
pbcopy < ~/.ssh/id_rsa.pub
```

Or simply with `cat ~/.ssh/id_rsa.pub` and then copy from the terminal output.

use standard output to copy the public key to your clipboard and then put it to clipboard

```
cat ~/.ssh/id_rsa.pub | pbcopy
```

copy to cliboard without pbcopy

```
cat ~/.ssh/id_rsa.pub
```


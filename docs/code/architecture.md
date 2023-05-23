---
title: "System architecture"
last_modified: "2023-05-07"
---
> Last modified: {{ page.last_modified | date: "%Y-%m-%d"}}

### Data and analysis
All data and analysis are kept on the `lenny` machine in this repository:
```
/home/data/apps/topfibers/
```

Note that this projects [repository](https://github.com/mr-devs/top-fibers) is cloned via `git` while _inside of the `topfibers/` directory_ and the name of the repository is specified as `repo`. 

E.g. via:
```
git clone git@github.com:mr-devs/top-FIBers.git repo
```

### Database and website
The database and the website code are kept on `lisa`.